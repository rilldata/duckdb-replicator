package duckdbreplicator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/marcboeker/go-duckdb"
	"gocloud.dev/blob"
)

type DB interface {
	// Close closes the database.
	Close() error

	// Query executes a query that returns rows typically a SELECT.
	Query(ctx context.Context, query string, args ...any) (*Rows, error)

	// CreateTableAsSelect creates a new table by name from the results of the given SQL query.
	CreateTableAsSelect(ctx context.Context, name string, sql string, opts *CreateTableOptions) error

	// InsertTableAsSelect inserts the results of the given SQL query into the table.
	InsertTableAsSelect(ctx context.Context, name string, sql string, opts *InsertTableOptions) error

	// DropTable removes a table from the database.
	DropTable(ctx context.Context, name string) error

	// RenameTable renames a table in the database.
	RenameTable(ctx context.Context, oldName, newName string) error

	// AddTableColumn adds a column to the table.
	AddTableColumn(ctx context.Context, tableName, columnName, typ string) error

	// AlterTableColumn alters the type of a column in the table.
	AlterTableColumn(ctx context.Context, tableName, columnName, newType string) error

	// Sync synchronizes the database with the cloud storage.
	Sync(ctx context.Context) error
}

type DBOptions struct {
	// Clean specifies whether to start with a clean database or download data from cloud storage and start with backed up data.
	Clean bool
	// LocalPath is the path where local db files will be stored. Should be unique for each database.
	LocalPath string

	BackupProvider *BackupProvider

	// BackupFormat is the format to use when backing up the database.
	BackupFormat BackupFormat

	// ReadSettings are settings applied the read duckDB handle.
	ReadSettings map[string]string
	// WriteSettings are settings applied the write duckDB handle.
	WriteSettings map[string]string
	// InitQueries are the queries to run when the database is first created.
	InitQueries []string

	StableSelect bool

	// LogLevel for the logs. Default: "debug".
	Logger *slog.Logger
}

type Rows struct {
	*sql.Rows
	cleanUp func()
}

func (q *Rows) Close() error {
	q.cleanUp()
	return q.Rows.Close()
}

type CreateTableOptions struct {
	// View specifies whether the created table is a view.
	View bool
}

type IncrementalStrategy string

const (
	IncrementalStrategyUnspecified IncrementalStrategy = ""
	IncrementalStrategyAppend      IncrementalStrategy = "append"
	IncrementalStrategyMerge       IncrementalStrategy = "merge"
)

type InsertTableOptions struct {
	ByName    bool
	Strategy  IncrementalStrategy
	UniqueKey []string
}

// NewDB creates a new DB instance.
// This can be a slow operation if the backup is large.
func NewDB(ctx context.Context, dbIdentifier string, opts *DBOptions) (DB, error) {
	if dbIdentifier == "" {
		return nil, fmt.Errorf("db identifier cannot be empty")
	}
	// TODO :: support clean run
	// For now deleting remote data is equivalent to clean run
	db := &db{
		opts:       opts,
		readPath:   filepath.Join(opts.LocalPath, dbIdentifier, "read"),
		writePath:  filepath.Join(opts.LocalPath, dbIdentifier, "write"),
		writeDirty: true,
		logger:     opts.Logger,
	}
	if opts.BackupProvider != nil {
		db.backup = blob.PrefixedBucket(opts.BackupProvider.bucket, dbIdentifier)
	}
	// create read and write paths
	err := os.MkdirAll(db.readPath, fs.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("unable to create read path: %w", err)
	}
	err = os.MkdirAll(db.writePath, fs.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("unable to create write path: %w", err)
	}

	// sync write path
	err = db.syncWrite(ctx)
	if err != nil {
		return nil, err
	}

	// sync read path
	err = db.Sync(ctx)
	if err != nil {
		return nil, err
	}

	// create read handle
	db.readHandle, err = db.openDBAndAttach(ctx, db.readPath, opts.ReadSettings, true)
	if err != nil {
		return nil, err
	}

	return db, nil
}

type db struct {
	opts *DBOptions

	readHandle *sql.DB
	readPath   string
	writePath  string
	readMu     sync.RWMutex
	writeMu    sync.Mutex
	writeDirty bool

	backup *blob.Bucket

	logger *slog.Logger
}

var _ DB = &db{}

func (d *db) Close() error {
	d.readMu.Lock()
	defer d.readMu.Unlock()

	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	return d.readHandle.Close()
}

// Query implements DB.
func (d *db) Query(ctx context.Context, query string, args ...any) (*Rows, error) {
	d.readMu.RLock()

	res, err := d.readHandle.QueryContext(ctx, query, args...)
	if err != nil {
		d.readMu.RUnlock()
		return nil, err
	}

	return &Rows{
		Rows:    res,
		cleanUp: d.readMu.RUnlock,
	}, nil

}

func (d *db) CreateTableAsSelect(ctx context.Context, name string, sql string, opts *CreateTableOptions) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	if opts == nil {
		opts = &CreateTableOptions{}
	}
	d.logger.Info("create table", slog.String("name", name), slog.Bool("view", opts.View))

	writeHandle, err := d.openDBAndAttach(ctx, d.writePath, d.opts.WriteSettings, false)
	if err != nil {
		return err
	}
	// Does not detaching attached DBs and directly closing the handle leak memory ?
	defer writeHandle.Close()

	// check if some older version exists
	oldVersion, oldVersionExists, _ := tableVersion(d.writePath, name)
	d.logger.Info("old version", slog.String("version", oldVersion), slog.Bool("exists", oldVersionExists))

	// create new version directory
	newVersion := newVersion()
	newVersionDir := filepath.Join(d.writePath, name, newVersion)
	err = os.MkdirAll(newVersionDir, fs.ModePerm)
	if err != nil {
		return fmt.Errorf("create: unable to create dir %q: %w", name, err)
	}

	var m meta
	if opts.View {
		// create view - validates that SQL is correct
		_, err = writeHandle.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS (%s\n)", safeSQLName(name), sql))
		if err != nil {
			return err
		}

		m = meta{ViewSQL: sql}
	} else {
		// create db file
		dbFile := filepath.Join(newVersionDir, "data.db")
		safeDBName := safeSQLName(dbName(name))

		// detach existing db
		_, err = writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE IF EXISTS %s", safeDBName), nil)
		if err != nil {
			_ = os.RemoveAll(newVersionDir)
			return fmt.Errorf("create: detach %q db failed: %w", safeDBName, err)
		}

		// attach new db
		_, err = writeHandle.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s", safeSQLString(dbFile), safeDBName), nil)
		if err != nil {
			_ = os.RemoveAll(newVersionDir)
			return fmt.Errorf("create: attach %q db failed: %w", dbFile, err)
		}

		// ingest data
		_, err = writeHandle.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE TABLE %s.default AS (%s\n)", safeDBName, sql), nil)
		if err != nil {
			_ = os.RemoveAll(newVersionDir)
			_, _ = writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", safeDBName))
			return fmt.Errorf("create: create %q.default table failed: %w", safeDBName, err)
		}

		m = meta{Format: BackupFormatDB}
	}

	d.writeDirty = true
	// write meta
	err = writeMeta(newVersionDir, m)
	if err != nil {
		_ = os.RemoveAll(newVersionDir)
		return err
	}

	// update version.txt
	err = os.WriteFile(filepath.Join(d.writePath, name, "version.txt"), []byte(newVersion), fs.ModePerm)
	if err != nil {
		return fmt.Errorf("create: write version file failed: %w", err)
	}

	// close write handle before syncing read so that temp files or wal files if any are removed
	err = writeHandle.Close()
	if err != nil {
		return err
	}

	if err := d.syncBackup(ctx, name); err != nil {
		// A minor optimisation to revert to earlier version can be done
		// but let syncFromBackup handle since we mark writeDirty as true
		return fmt.Errorf("create: replicate failed: %w", err)
	}
	d.logger.Info("table created", slog.String("name", name))

	if oldVersionExists {
		_ = os.RemoveAll(filepath.Join(d.writePath, name, oldVersion))
		_ = d.deleteBackupTable(ctx, name, oldVersion)
	}
	// both backups and write are now in sync
	d.writeDirty = false
	return d.Sync(ctx)
}

func (d *db) InsertTableAsSelect(ctx context.Context, name string, sql string, opts *InsertTableOptions) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	d.logger.Info("insert table", slog.String("name", name), slog.Group("option", "by_name", opts.ByName, "strategy", string(opts.Strategy), "unique_key", opts.UniqueKey))
	// Get current table version
	oldVersion, oldVersionExists, err := tableVersion(d.writePath, name)
	if err != nil || !oldVersionExists {
		return fmt.Errorf("table %q does not exist", name)
	}

	writeHandle, err := d.initWriteHandle(ctx)
	if err != nil {
		return err
	}

	d.writeDirty = true
	// Execute the insert
	err = d.execIncrementalInsert(ctx, writeHandle, name, sql, opts)
	if err != nil {
		return fmt.Errorf("insert: insert into table %q failed: %w", name, err)
	}

	// rename db directory
	newVersion := newVersion()
	oldVersionDir := filepath.Join(d.writePath, name, oldVersion)
	err = os.Rename(oldVersionDir, filepath.Join(d.writePath, name, newVersion))
	if err != nil {
		return fmt.Errorf("insert: update version %q failed: %w", newVersion, err)
	}

	// update version.txt
	err = os.WriteFile(filepath.Join(d.writePath, name, "version.txt"), []byte(newVersion), fs.ModePerm)
	if err != nil {
		return fmt.Errorf("insert: write version file failed: %w", err)
	}

	err = writeHandle.Close()
	if err != nil {
		return err
	}
	// replicate
	err = d.syncBackup(ctx, name)
	if err != nil {
		return fmt.Errorf("insert: replicate failed: %w", err)
	}
	// both backups and write are now in sync
	d.writeDirty = false

	// Delete the old version (ignoring errors since source the new data has already been correctly inserted)
	_ = os.RemoveAll(oldVersionDir)
	_ = d.deleteBackupTable(ctx, name, oldVersion)
	return d.Sync(ctx)
}

// DropTable implements DB.
func (d *db) DropTable(ctx context.Context, name string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	d.logger.Info("drop table", slog.String("name", name))
	_, exist, _ := tableVersion(d.writePath, name)
	if !exist {
		return fmt.Errorf("drop: table %q not found", name)
	}

	// sync from backup
	err := d.syncWrite(ctx)
	if err != nil {
		return err
	}

	d.writeDirty = true
	// delete the table directory
	err = os.RemoveAll(filepath.Join(d.writePath, name))
	if err != nil {
		return fmt.Errorf("drop: unable to drop table %q: %w", name, err)
	}

	// drop the table from backup location
	err = d.deleteBackupTable(ctx, name, "")
	if err != nil {
		return fmt.Errorf("drop: unable to drop table %q from backup: %w", name, err)
	}
	// both backups and write are now in sync
	d.writeDirty = false
	return d.Sync(ctx)
}

func (d *db) RenameTable(ctx context.Context, oldName, newName string) error {
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	d.logger.Info("rename table", slog.String("from", oldName), slog.String("to", newName))
	if strings.EqualFold(oldName, newName) {
		return fmt.Errorf("rename: Table with name %q already exists", newName)
	}

	// sync from backup
	err := d.syncWrite(ctx)
	if err != nil {
		return err
	}

	oldVersion, exist, err := d.writeTableVersion(oldName)
	if err != nil {
		return err
	}
	if !exist {
		return fmt.Errorf("rename: Table %q not found", oldName)
	}

	oldVersionInNewDir, replaceInNewTable, err := d.writeTableVersion(newName)
	if err != nil {
		return err
	}
	if !replaceInNewTable {
		err = os.Mkdir(filepath.Join(d.writePath, newName), fs.ModePerm)
		if err != nil {
			return err
		}
	}

	newVersion := fmt.Sprint(time.Now().UnixMilli())
	d.writeDirty = true
	err = os.Rename(filepath.Join(d.writePath, oldName, oldVersion), filepath.Join(d.writePath, newName, newVersion))
	if err != nil {
		return fmt.Errorf("rename: rename file failed: %w", err)
	}

	writeErr := os.WriteFile(filepath.Join(d.writePath, newName, "version.txt"), []byte(newVersion), fs.ModePerm)
	if writeErr != nil {
		return fmt.Errorf("rename: write version file failed: %w", writeErr)
	}

	err = os.RemoveAll(filepath.Join(d.writePath, oldName))
	if err != nil {
		d.logger.Error("rename: unable to delete old path", slog.Any("error", err))
	}

	if replaceInNewTable {
		// new table had some other file previously
		err = os.RemoveAll(filepath.Join(d.writePath, newName, oldVersionInNewDir))
		if err != nil {
			d.logger.Error("rename: unable to delete old version of new table", slog.Any("error", err))
		}
	}

	if d.syncBackup(ctx, newName) != nil {
		return fmt.Errorf("rename: unable to replicate new table")
	}
	d.writeDirty = false
	return d.Sync(ctx)
}

func (d *db) AddTableColumn(ctx context.Context, tableName, columnName, typ string) error {
	d.logger.Info("AddTableColumn", slog.String("table", tableName), slog.String("column", columnName), slog.String("typ", typ))
	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	version, exist, err := tableVersion(d.writePath, tableName)
	if err != nil {
		return err
	}

	if !exist {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	writeHandle, err := d.initWriteHandle(ctx)
	if err != nil {
		return err
	}

	d.writeDirty = true
	_, err = writeHandle.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.default ADD COLUMN %s %s", safeSQLName(dbName(tableName)), safeSQLName(columnName), typ))
	writeHandle.Close()
	if err != nil {
		return err
	}

	// rename to new version
	newVersion := fmt.Sprint(time.Now().UnixMilli())
	err = os.Rename(filepath.Join(d.writePath, tableName, version), filepath.Join(d.writePath, tableName, newVersion))
	if err != nil {
		return err
	}

	// update version.txt
	err = os.WriteFile(filepath.Join(d.writePath, tableName, "version.txt"), []byte(newVersion), fs.ModePerm)
	if err != nil {
		return err
	}

	// replicate
	err = d.syncBackup(ctx, tableName)
	if err != nil {
		return err
	}
	d.writeDirty = false
	return nil
}

// AlterTableColumn implements drivers.OLAPStore.
func (d *db) AlterTableColumn(ctx context.Context, tableName, columnName, newType string) error {
	d.logger.Info("AlterTableColumn", slog.String("table", tableName), slog.String("column", columnName), slog.String("typ", newType))

	d.writeMu.Lock()
	defer d.writeMu.Unlock()

	version, exist, err := tableVersion(d.writePath, tableName)
	if err != nil {
		return err
	}

	if !exist {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	writeHandle, err := d.initWriteHandle(ctx)
	if err != nil {
		return err
	}

	d.writeDirty = true
	_, err = writeHandle.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.default ALTER %s TYPE %s", safeSQLName(dbName(tableName)), safeSQLName(columnName), newType))
	writeHandle.Close()
	if err != nil {
		return err
	}

	// rename to new version
	newVersion := fmt.Sprint(time.Now().UnixMilli())
	err = os.Rename(filepath.Join(d.writePath, tableName, version), filepath.Join(d.writePath, tableName, newVersion))
	if err != nil {
		return err
	}

	// update version.txt
	err = os.WriteFile(filepath.Join(d.writePath, tableName, "version.txt"), []byte(newVersion), fs.ModePerm)
	if err != nil {
		return err
	}

	// replicate
	err = d.syncBackup(ctx, tableName)
	if err != nil {
		return err
	}
	d.writeDirty = false
	return nil
}

// Sync implements DB.
func (d *db) Sync(ctx context.Context) error {
	entries, err := os.ReadDir(d.writePath)
	if err != nil {
		return err
	}

	tables := make(map[string]any)
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		tables[entry.Name()] = nil

		// Check if there is already a table with the same version
		writeVersion, exist, _ := d.writeTableVersion(entry.Name())
		if !exist {
			continue
		}
		readVersion, _, _ := d.readTableVersion(entry.Name())
		if writeVersion == readVersion {
			continue
		}

		readPath := filepath.Join(d.readPath, entry.Name())

		// clear read path
		err = os.RemoveAll(readPath)
		if err != nil {
			return err
		}

		d.logger.Info("Sync: copying table", slog.String("table", entry.Name()))
		err = copyDir(readPath, filepath.Join(d.writePath, entry.Name()))
		if err != nil {
			return err
		}

	}

	// delete data for tables that have been removed from write
	entries, err = os.ReadDir(d.readPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		_, ok := tables[entry.Name()]
		if ok {
			continue
		}
		d.logger.Info("Sync: removing table", slog.String("table", entry.Name()))
		err = os.RemoveAll(filepath.Join(d.readPath, entry.Name()))
		if err != nil {
			return err
		}
	}

	handle, err := d.openDBAndAttach(ctx, d.readPath, d.opts.ReadSettings, true)
	if err != nil {
		return err
	}

	var oldDBHandle *sql.DB
	d.readMu.Lock()
	// swap read handle
	oldDBHandle = d.readHandle
	d.readHandle = handle
	d.readMu.Unlock()

	// close old read handle
	if oldDBHandle != nil {
		err = oldDBHandle.Close()
		if err != nil {
			d.logger.Error("error in closing old read handle", slog.String("error", err.Error()))
		}
	}
	return nil
}

// initWriteHandle syncs the write database and initializes the write handle.
// Should be called only with writeMu locked.
func (d *db) initWriteHandle(ctx context.Context) (*sql.DB, error) {
	err := d.syncWrite(ctx)
	if err != nil {
		return nil, err
	}

	return d.openDBAndAttach(ctx, d.writePath, d.opts.WriteSettings, false)
}

func (d *db) execIncrementalInsert(ctx context.Context, h *sql.DB, name, sql string, opts *InsertTableOptions) error {
	var byNameClause string
	if opts.ByName {
		byNameClause = "BY NAME"
	}

	safeName := fmt.Sprintf("%s.default", safeSQLName(dbName(name)))
	if opts.Strategy == IncrementalStrategyAppend {
		_, err := h.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s %s (%s\n)", safeName, byNameClause, sql))
		return err
	}

	conn, err := h.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if opts.Strategy == IncrementalStrategyMerge {
		// Create a temporary table with the new data
		tmp := uuid.New().String()
		_, err := conn.ExecContext(ctx, fmt.Sprintf("CREATE TEMPORARY TABLE %s AS (%s\n)", safeSQLName(tmp), sql))
		if err != nil {
			return err
		}

		// check the count of the new data
		// skip if the count is 0
		// if there was no data in the empty file then the detected schema can be different from the current schema which leads to errors or performance issues
		res := conn.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) == 0 FROM %s", safeSQLName(tmp)))
		var empty bool
		if err := res.Scan(&empty); err != nil {
			return err
		}

		if empty {
			return nil
		}

		// Drop the rows from the target table where the unique key is present in the temporary table
		where := ""
		for i, key := range opts.UniqueKey {
			key = safeSQLName(key)
			if i != 0 {
				where += " AND "
			}
			where += fmt.Sprintf("base.%s IS NOT DISTINCT FROM tmp.%s", key, key)
		}
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s base WHERE EXISTS (SELECT 1 FROM %s tmp WHERE %s)", safeName, safeSQLName(tmp), where))
		if err != nil {
			return err
		}

		// Insert the new data into the target table
		_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s %s SELECT * FROM %s", safeName, byNameClause, safeSQLName(tmp)))
		if err != nil {
			return err
		}
	}

	return fmt.Errorf("incremental insert strategy %q not supported", opts.Strategy)
}

func (d *db) openDBAndAttach(ctx context.Context, path string, settings map[string]string, read bool) (*sql.DB, error) {
	// open the db
	var dsn *url.URL
	var err error
	if read {
		dsn, err = url.Parse("")
	} else {
		dsn, err = url.Parse(filepath.Join(path, "stage.db"))
	}
	if err != nil {
		return nil, err
	}

	query := dsn.Query()
	for k, v := range settings {
		query.Set(k, v)
	}
	dsn.RawQuery = query.Encode()
	connector, err := duckdb.NewConnector(dsn.String(), func(execer driver.ExecerContext) error {
		for _, q := range d.opts.InitQueries {
			_, err := execer.ExecContext(ctx, q, nil)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)
	err = db.PingContext(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}

	err = d.attachDBs(ctx, db, path, read)
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (d *db) attachDBs(ctx context.Context, db *sql.DB, path string, read bool) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		version, exist, err := tableVersion(path, entry.Name())
		if err != nil {
			d.logger.Error("error in fetching db version", slog.String("table", entry.Name()), slog.Any("error", err))
			_ = os.RemoveAll(path)
			continue
		}
		if !exist {
			_ = os.RemoveAll(path)
			continue
		}
		path := filepath.Join(path, entry.Name(), version)

		// read meta file
		f, err := os.ReadFile(filepath.Join(path, "meta.json"))
		if err != nil {
			_ = os.RemoveAll(path)
			d.logger.Error("error in reading meta file", slog.String("table", entry.Name()), slog.Any("error", err))
			// May be keep it as a config to return error or continue ?
			continue
		}
		var meta meta
		err = json.Unmarshal(f, &meta)
		if err != nil {
			_ = os.RemoveAll(path)
			d.logger.Error("error in unmarshalling meta file", slog.String("table", entry.Name()), slog.Any("error", err))
			continue
		}

		if meta.ViewSQL != "" {
			// table is a view
			_, err := db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", safeSQLName(entry.Name()), meta.ViewSQL))
			if err != nil {
				return err
			}
			continue
		}
		switch BackupFormat(meta.Format) {
		case BackupFormatDB:
			dbName := dbName(entry.Name())
			var readMode string
			if read {
				readMode = " (READ_ONLY)"
			}
			_, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s %s", safeSQLString(filepath.Join(path, "data.db")), safeSQLName(dbName), readMode))
			if err != nil {
				d.logger.Error("error in attaching db", slog.String("table", entry.Name()), slog.Any("error", err))
				_ = os.RemoveAll(filepath.Join(path))
				continue
			}

			sql, err := d.generateSelectQuery(ctx, db, dbName)
			if err != nil {
				return err
			}

			_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", safeSQLName(entry.Name()), sql))
			if err != nil {
				return err
			}
		case BackupFormatParquet:
			panic("unimplemented")
		default:
			return fmt.Errorf("unknown backup format %q", meta.Format)
		}
	}
	return nil
}

func (d *db) generateSelectQuery(ctx context.Context, handle *sql.DB, dbName string) (string, error) {
	if !d.opts.StableSelect {
		return fmt.Sprintf("SELECT * FROM %s.default", dbName), nil
	}
	rows, err := handle.QueryContext(ctx, `
			SELECT column_name AS name
			FROM information_schema.columns
			WHERE table_catalog = ? AND table_name = 'default'
			ORDER BY name ASC`, dbName)
	if err != nil {
		return "", err
	}
	defer rows.Close()

	cols := make([]string, 0)
	var col string
	for rows.Next() {
		if err := rows.Scan(&col); err != nil {
			return "", err
		}
		cols = append(cols, safeSQLName(col))
	}

	return fmt.Sprintf("SELECT %s FROM %s.default", strings.Join(cols, ", "), safeSQLName(dbName)), nil
}

func (d *db) readTableVersion(name string) (string, bool, error) {
	return tableVersion(d.readPath, name)
}

func (d *db) writeTableVersion(name string) (string, bool, error) {
	return tableVersion(d.writePath, name)
}

func tableVersion(path, name string) (string, bool, error) {
	pathToFile := filepath.Join(path, name, "version.txt")
	contents, err := os.ReadFile(pathToFile)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", false, nil
		}
		return "", false, err
	}
	return strings.TrimSpace(string(contents)), true, nil
}

func newVersion() string {
	return strconv.FormatInt(time.Now().UnixMilli(), 10)
}

type meta struct {
	ViewSQL string
	Format  BackupFormat
}

func writeMeta(path string, meta meta) error {
	metaBytes, err := json.Marshal(meta)
	if err != nil {
		return fmt.Errorf("create: marshal meta failed: %w", err)
	}
	err = os.WriteFile(filepath.Join(path, "meta.json"), metaBytes, fs.ModePerm)
	if err != nil {
		return fmt.Errorf("create: write meta failed: %w", err)
	}
	return nil
}

func retry(maxRetries int, delay time.Duration, fn func() error) error {
	var err error
	for i := 0; i < maxRetries; i++ {
		err = fn()
		if err == nil {
			return nil // success
		} else if strings.Contains(err.Error(), "stream error: stream ID") {
			time.Sleep(delay) // retry
		} else {
			break // return error
		}
	}
	return err
}

func dbName(name string) string {
	return safeSQLName(fmt.Sprintf("%s__data__db", name))
}
