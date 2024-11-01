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
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/XSAM/otelsql"
	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/marcboeker/go-duckdb"
	"github.com/mitchellh/mapstructure"
	"go.opentelemetry.io/otel/attribute"
	"gocloud.dev/blob"
)

type DB interface {
	// Close closes the database.
	Close() error

	// AcquireReadConnection returns a connection to the database for reading.
	// Once done the connection should be released by calling the release function.
	// This connection must only be used for select queries or for creating and working with temporary tables.
	// Prefer Query instead of using this method directly for select queries.
	AcquireReadConnection(ctx context.Context) (conn Conn, release func() error, err error)

	// AcquireWriteConnection returns a connection to the database for writing.
	// Once done the connection should be released by calling the release function.
	// Any persistent changes to the database should be done by calling CRUD APIs only.
	AcquireWriteConnection(ctx context.Context) (conn Conn, release func() error, err error)

	// Sync synchronizes the database with the cloud storage.
	Sync(ctx context.Context) error

	// CRUD APIs

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

// TODO :: revisit this logic
func (d *DBOptions) ValidateSettings() error {
	read := &settings{}
	err := mapstructure.Decode(d.ReadSettings, read)
	if err != nil {
		return fmt.Errorf("read settings: %w", err)
	}

	write := &settings{}
	err = mapstructure.Decode(d.WriteSettings, write)
	if err != nil {
		return fmt.Errorf("write settings: %w", err)
	}

	// no memory limits defined
	// divide memory equally between read and write
	if read.MaxMemory == "" && write.MaxMemory == "" {
		connector, err := duckdb.NewConnector("", nil)
		if err != nil {
			return fmt.Errorf("unable to create duckdb connector: %w", err)
		}
		defer connector.Close()
		db := sql.OpenDB(connector)
		defer db.Close()

		row := db.QueryRow("SELECT value FROM duckdb_settings() WHERE name = 'max_memory'")
		var maxMemory string
		err = row.Scan(&maxMemory)
		if err != nil {
			return fmt.Errorf("unable to get max_memory: %w", err)
		}

		bytes, err := humanReadableSizeToBytes(maxMemory)
		if err != nil {
			return fmt.Errorf("unable to parse max_memory: %w", err)
		}

		read.MaxMemory = fmt.Sprintf("%d bytes", int64(bytes)/2)
		write.MaxMemory = fmt.Sprintf("%d bytes", int64(bytes)/2)
	}

	if read.MaxMemory == "" != (write.MaxMemory == "") {
		// only one is defined
		var mem string
		if read.MaxMemory != "" {
			mem = read.MaxMemory
		} else {
			mem = write.MaxMemory
		}

		bytes, err := humanReadableSizeToBytes(mem)
		if err != nil {
			return fmt.Errorf("unable to parse max_memory: %w", err)
		}

		read.MaxMemory = fmt.Sprintf("%d bytes", int64(bytes)/2)
		write.MaxMemory = fmt.Sprintf("%d bytes", int64(bytes)/2)
	}

	var readThread, writeThread int
	if read.Threads != "" {
		readThread, err = strconv.Atoi(read.Threads)
		if err != nil {
			return fmt.Errorf("unable to parse read threads: %w", err)
		}
	}
	if write.Threads != "" {
		writeThread, err = strconv.Atoi(write.Threads)
		if err != nil {
			return fmt.Errorf("unable to parse write threads: %w", err)
		}
	}

	if readThread == 0 && writeThread == 0 {
		connector, err := duckdb.NewConnector("", nil)
		if err != nil {
			return fmt.Errorf("unable to create duckdb connector: %w", err)
		}
		defer connector.Close()
		db := sql.OpenDB(connector)
		defer db.Close()

		row := db.QueryRow("SELECT value FROM duckdb_settings() WHERE name = 'threads'")
		var threads int
		err = row.Scan(&threads)
		if err != nil {
			return fmt.Errorf("unable to get threads: %w", err)
		}

		read.Threads = strconv.Itoa(threads / 2)
		write.Threads = strconv.Itoa(threads / 2)
	}

	if readThread == 0 != (writeThread == 0) {
		// only one is defined
		var threads int
		if readThread != 0 {
			threads = readThread
		} else {
			threads = writeThread
		}

		read.Threads = strconv.Itoa(threads / 2)
		write.Threads = strconv.Itoa(threads / 2)
	}

	err = mapstructure.WeakDecode(read, &d.ReadSettings)
	if err != nil {
		return fmt.Errorf("failed to update read settings: %w", err)
	}

	err = mapstructure.WeakDecode(write, &d.WriteSettings)
	if err != nil {
		return fmt.Errorf("failed to update write settings: %w", err)
	}
	return nil
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
// dbIdentifier is a unique identifier for the database reported in metrics.
func NewDB(ctx context.Context, dbIdentifier string, opts *DBOptions) (DB, error) {
	if dbIdentifier == "" {
		return nil, fmt.Errorf("db identifier cannot be empty")
	}
	err := opts.ValidateSettings()
	if err != nil {
		return nil, err
	}
	db := &db{
		dbIdentifier: dbIdentifier,
		opts:         opts,
		readPath:     filepath.Join(opts.LocalPath, "read"),
		writePath:    filepath.Join(opts.LocalPath, "write"),
		writeDirty:   true,
		logger:       opts.Logger,
	}
	if opts.BackupProvider != nil {
		db.backup = opts.BackupProvider.bucket
	}
	// if clean is true, remove the backup
	if opts.Clean {
		err = db.deleteBackup(ctx, "", "")
		if err != nil {
			return nil, fmt.Errorf("unable to clean backup: %w", err)
		}
	}

	// create read and write paths
	err = os.MkdirAll(db.readPath, fs.ModePerm)
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
	dbIdentifier string
	opts         *DBOptions

	readHandle *sqlx.DB
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

func (d *db) AcquireReadConnection(ctx context.Context) (Conn, func() error, error) {
	d.readMu.RLock()

	c, err := d.readHandle.Connx(ctx)
	if err != nil {
		d.readMu.RUnlock()
		return nil, nil, err
	}

	return &conn{
			Conn: c,
			db:   d,
		}, func() error {
			err = c.Close()
			d.readMu.RUnlock()
			return err
		}, nil
}

func (d *db) AcquireWriteConnection(ctx context.Context) (Conn, func() error, error) {
	c, release, err := d.acquireWriteConn(ctx)
	if err != nil {
		return nil, nil, err
	}

	return &conn{
		Conn: c,
		db:   d,
	}, release, nil

}

func (d *db) CreateTableAsSelect(ctx context.Context, name string, sql string, opts *CreateTableOptions) error {
	if opts == nil {
		opts = &CreateTableOptions{}
	}
	d.logger.Info("create table", slog.String("name", name), slog.Bool("view", opts.View))
	conn, release, err := d.acquireWriteConn(ctx)
	if err != nil {
		return err
	}
	defer release()
	return d.createTableAsSelect(ctx, conn, release, name, sql, opts)
}

func (d *db) createTableAsSelect(ctx context.Context, conn *sqlx.Conn, releaseConn func() error, name, sql string, opts *CreateTableOptions) error {

	// check if some older version exists
	oldVersion, oldVersionExists, _ := tableVersion(d.writePath, name)
	d.logger.Info("old version", slog.String("version", oldVersion), slog.Bool("exists", oldVersionExists))

	// create new version directory
	newVersion := newVersion()
	newVersionDir := filepath.Join(d.writePath, name, newVersion)
	err := os.MkdirAll(newVersionDir, fs.ModePerm)
	if err != nil {
		return fmt.Errorf("create: unable to create dir %q: %w", name, err)
	}

	var m meta
	if opts.View {
		// create view - validates that SQL is correct
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS (%s\n)", safeSQLName(name), sql))
		if err != nil {
			return err
		}

		m = meta{ViewSQL: sql}
	} else {
		// create db file
		dbFile := filepath.Join(newVersionDir, "data.db")
		safeDBName := safeSQLName(dbName(name))

		// detach existing db
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE IF EXISTS %s", safeDBName), nil)
		if err != nil {
			_ = os.RemoveAll(newVersionDir)
			return fmt.Errorf("create: detach %q db failed: %w", safeDBName, err)
		}

		// attach new db
		_, err = conn.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s", safeSQLString(dbFile), safeDBName), nil)
		if err != nil {
			_ = os.RemoveAll(newVersionDir)
			return fmt.Errorf("create: attach %q db failed: %w", dbFile, err)
		}

		// ingest data
		_, err = conn.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE TABLE %s.default AS (%s\n)", safeDBName, sql), nil)
		if err != nil {
			_ = os.RemoveAll(newVersionDir)
			_, _ = conn.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", safeDBName))
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
	err = releaseConn()
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
		_ = d.deleteBackup(ctx, name, oldVersion)
	}
	// both backups and write are now in sync
	d.writeDirty = false
	return d.Sync(ctx)
}

func (d *db) InsertTableAsSelect(ctx context.Context, name string, sql string, opts *InsertTableOptions) error {
	if opts == nil {
		opts = &InsertTableOptions{
			Strategy: IncrementalStrategyAppend,
		}
	}

	d.logger.Info("insert table", slog.String("name", name), slog.Group("option", "by_name", opts.ByName, "strategy", string(opts.Strategy), "unique_key", opts.UniqueKey))
	conn, release, err := d.acquireWriteConn(ctx)
	if err != nil {
		return err
	}
	defer release()
	return d.insertTableAsSelect(ctx, conn, release, name, sql, opts)
}

func (d *db) insertTableAsSelect(ctx context.Context, conn *sqlx.Conn, releaseConn func() error, name, sql string, opts *InsertTableOptions) error {
	// Get current table version
	oldVersion, oldVersionExists, err := tableVersion(d.writePath, name)
	if err != nil || !oldVersionExists {
		return fmt.Errorf("table %q does not exist", name)
	}

	d.writeDirty = true
	// Execute the insert
	err = execIncrementalInsert(ctx, conn, fmt.Sprintf("%s.default", safeSQLName(dbName(name))), sql, opts)
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

	err = releaseConn()
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
	_ = d.deleteBackup(ctx, name, oldVersion)
	return d.Sync(ctx)
}

// DropTable implements DB.
func (d *db) DropTable(ctx context.Context, name string) error {
	d.logger.Info("drop table", slog.String("name", name))
	_, release, err := d.acquireWriteConn(ctx) // we don't need the handle but need to sync the write pah
	if err != nil {
		return err
	}
	defer release()

	return d.dropTable(ctx, name)
}

func (d *db) dropTable(ctx context.Context, name string) error {
	_, exist, _ := tableVersion(d.writePath, name)
	if !exist {
		return fmt.Errorf("drop: table %q not found", name)
	}

	d.writeDirty = true
	// delete the table directory
	err := os.RemoveAll(filepath.Join(d.writePath, name))
	if err != nil {
		return fmt.Errorf("drop: unable to drop table %q: %w", name, err)
	}

	// drop the table from backup location
	err = d.deleteBackup(ctx, name, "")
	if err != nil {
		return fmt.Errorf("drop: unable to drop table %q from backup: %w", name, err)
	}
	// both backups and write are now in sync
	d.writeDirty = false
	return d.Sync(ctx)
}

func (d *db) RenameTable(ctx context.Context, oldName, newName string) error {
	d.logger.Info("rename table", slog.String("from", oldName), slog.String("to", newName))
	if strings.EqualFold(oldName, newName) {
		return fmt.Errorf("rename: Table with name %q already exists", newName)
	}

	_, release, err := d.acquireWriteConn(ctx) // we don't need the handle but need to sync the write
	if err != nil {
		return err
	}
	defer release()

	return d.renameTable(ctx, oldName, newName)
}

func (d *db) renameTable(ctx context.Context, oldName, newName string) error {
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
		err = d.deleteBackup(ctx, newName, oldVersionInNewDir)
		if err != nil {
			d.logger.Error("rename: unable to delete old version of new table from backup", slog.Any("error", err))
		}
	}
	err = d.deleteBackup(ctx, oldName, "")
	if err != nil {
		d.logger.Error("rename: unable to delete old table from backup", slog.Any("error", err))
	}
	if d.syncBackup(ctx, newName) != nil {
		return fmt.Errorf("rename: unable to replicate new table")
	}
	d.writeDirty = false
	return d.Sync(ctx)
}

func (d *db) AddTableColumn(ctx context.Context, tableName, columnName, typ string) error {
	d.logger.Info("AddTableColumn", slog.String("table", tableName), slog.String("column", columnName), slog.String("typ", typ))
	conn, release, err := d.acquireWriteConn(ctx)
	if err != nil {
		return err
	}
	defer release()

	return d.addTableColumn(ctx, conn, release, tableName, columnName, typ)
}

func (d *db) addTableColumn(ctx context.Context, conn *sqlx.Conn, releaseConn func() error, tableName, columnName, typ string) error {
	version, exist, err := tableVersion(d.writePath, tableName)
	if err != nil {
		return err
	}

	if !exist {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	d.writeDirty = true
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.default ADD COLUMN %s %s", safeSQLName(dbName(tableName)), safeSQLName(columnName), typ))
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

	err = releaseConn()
	if err != nil {
		return err
	}

	// replicate
	err = d.syncBackup(ctx, tableName)
	if err != nil {
		return err
	}
	d.writeDirty = false
	return d.Sync(ctx)
}

// AlterTableColumn implements drivers.OLAPStore.
func (d *db) AlterTableColumn(ctx context.Context, tableName, columnName, newType string) error {
	d.logger.Info("AlterTableColumn", slog.String("table", tableName), slog.String("column", columnName), slog.String("typ", newType))
	conn, release, err := d.acquireWriteConn(ctx)
	if err != nil {
		return err
	}
	defer release()

	return d.alterTableColumn(ctx, conn, release, tableName, columnName, newType)
}

func (d *db) alterTableColumn(ctx context.Context, conn *sqlx.Conn, releaseConn func() error, tableName, columnName, newType string) error {
	version, exist, err := tableVersion(d.writePath, tableName)
	if err != nil {
		return err
	}

	if !exist {
		return fmt.Errorf("table %q does not exist", tableName)
	}

	d.writeDirty = true
	_, err = conn.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s.default ALTER %s TYPE %s", safeSQLName(dbName(tableName)), safeSQLName(columnName), newType))
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

	err = releaseConn()
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

	var oldDBHandle *sqlx.DB
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

// acquireWriteConn syncs the write database, initializes the write handle and returns a write connection.
func (d *db) acquireWriteConn(ctx context.Context) (*sqlx.Conn, func() error, error) {
	d.writeMu.Lock()
	err := d.syncWrite(ctx)
	if err != nil {
		d.writeMu.Unlock()
		return nil, nil, err
	}

	db, err := d.openDBAndAttach(ctx, d.writePath, d.opts.WriteSettings, false)
	if err != nil {
		d.writeMu.Unlock()
		return nil, nil, err
	}
	conn, err := db.Connx(ctx)
	if err != nil {
		_ = db.Close()
		d.writeMu.Unlock()
		return nil, nil, err
	}
	release := false
	return conn, func() error {
		if release { // make release idempotent
			return nil
		}
		_ = conn.Close()
		err = db.Close()
		d.writeMu.Unlock()
		release = true
		return err
	}, nil
}

func (d *db) openDBAndAttach(ctx context.Context, path string, settings map[string]string, read bool) (*sqlx.DB, error) {
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
		for _, qry := range d.opts.InitQueries {
			_, err := execer.ExecContext(context.Background(), qry, nil)
			if err != nil && strings.Contains(err.Error(), "Failed to download extension") {
				// Retry using another mirror. Based on: https://github.com/duckdb/duckdb/issues/9378
				_, err = execer.ExecContext(context.Background(), qry+" FROM 'http://nightly-extensions.duckdb.org'", nil)
			}
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		// Check for using incompatible database files
		if strings.Contains(err.Error(), "Trying to read a database file with version number") {
			return nil, err
			// TODO :: fix
			// return nil, fmt.Errorf("database file %q was created with an older, incompatible version of Rill (please remove it and try again)", c.config.DSN)
		}

		// Check for another process currently accessing the DB
		if strings.Contains(err.Error(), "Could not set lock on file") {
			return nil, fmt.Errorf("failed to open database (is Rill already running?): %w", err)
		}

		return nil, err
	}

	db := sqlx.NewDb(otelsql.OpenDB(connector), "duckdb")
	// TODO :: Do we need to limit max open connections ?
	// db.SetMaxOpenConns(c.config.PoolSize)

	err = otelsql.RegisterDBStatsMetrics(db.DB, otelsql.WithAttributes(attribute.String("db.system", "duckdb"), attribute.String("db_identifier", d.dbIdentifier)))
	if err != nil {
		return nil, fmt.Errorf("registering db stats metrics: %w", err)
	}

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

	// 2023-12-11: Hail mary for solving this issue: https://github.com/duckdblabs/rilldata/issues/6.
	// Forces DuckDB to create catalog entries for the information schema up front (they are normally created lazily).
	// Can be removed if the issue persists.
	_, err = db.ExecContext(context.Background(), `
		select
			coalesce(t.table_catalog, current_database()) as "database",
			t.table_schema as "schema",
			t.table_name as "name",
			t.table_type as "type", 
			array_agg(c.column_name order by c.ordinal_position) as "column_names",
			array_agg(c.data_type order by c.ordinal_position) as "column_types",
			array_agg(c.is_nullable = 'YES' order by c.ordinal_position) as "column_nullable"
		from information_schema.tables t
		join information_schema.columns c on t.table_schema = c.table_schema and t.table_name = c.table_name
		group by 1, 2, 3, 4
		order by 1, 2, 3, 4
	`)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (d *db) attachDBs(ctx context.Context, db *sqlx.DB, path string, read bool) error {
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	var views []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		version, exist, err := tableVersion(path, entry.Name())
		if err != nil {
			d.logger.Error("error in fetching db version", slog.String("table", entry.Name()), slog.Any("error", err))
			_ = os.RemoveAll(filepath.Join(path, entry.Name()))
			continue
		}
		if !exist {
			_ = os.RemoveAll(filepath.Join(path, entry.Name()))
			continue
		}
		versionPath := filepath.Join(path, entry.Name(), version)

		// read meta file
		f, err := os.ReadFile(filepath.Join(versionPath, "meta.json"))
		if err != nil {
			_ = os.RemoveAll(versionPath)
			d.logger.Error("error in reading meta file", slog.String("table", entry.Name()), slog.Any("error", err))
			// May be keep it as a config to return error or continue ?
			continue
		}
		var meta meta
		err = json.Unmarshal(f, &meta)
		if err != nil {
			_ = os.RemoveAll(versionPath)
			d.logger.Error("error in unmarshalling meta file", slog.String("table", entry.Name()), slog.Any("error", err))
			continue
		}

		if meta.ViewSQL != "" {
			// table is a view
			views = append(views, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS (%s\n)", safeSQLName(entry.Name()), meta.ViewSQL))
			continue
		}
		switch BackupFormat(meta.Format) {
		case BackupFormatDB:
			dbName := dbName(entry.Name())
			var readMode string
			if read {
				readMode = " (READ_ONLY)"
			}
			_, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s %s", safeSQLString(filepath.Join(versionPath, "data.db")), safeSQLName(dbName), readMode))
			if err != nil {
				d.logger.Error("error in attaching db", slog.String("table", entry.Name()), slog.Any("error", err))
				_ = os.RemoveAll(versionPath)
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
	// create views after attaching all the DBs since views can depend on other tables
	for _, view := range views {
		_, err := db.ExecContext(ctx, view)
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *db) generateSelectQuery(ctx context.Context, handle *sqlx.DB, dbName string) (string, error) {
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

func execIncrementalInsert(ctx context.Context, conn *sqlx.Conn, safeTableName, sql string, opts *InsertTableOptions) error {
	var byNameClause string
	if opts.ByName {
		byNameClause = "BY NAME"
	}

	if opts.Strategy == IncrementalStrategyAppend {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s %s (%s\n)", safeTableName, byNameClause, sql))
		return err
	}

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
		_, err = conn.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s base WHERE EXISTS (SELECT 1 FROM %s tmp WHERE %s)", safeTableName, safeSQLName(tmp), where))
		if err != nil {
			return err
		}

		// Insert the new data into the target table
		_, err = conn.ExecContext(ctx, fmt.Sprintf("INSERT INTO %s %s SELECT * FROM %s", safeTableName, byNameClause, safeSQLName(tmp)))
		return err
	}

	return fmt.Errorf("incremental insert strategy %q not supported", opts.Strategy)
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

func dbName(name string) string {
	return fmt.Sprintf("%s__data__db", name)
}

type settings struct {
	MaxMemory string `mapstructure:"max_memory"`
	Threads   string `mapstructure:"threads"`
	// Can be more settings
}

// Regex to parse human-readable size returned by DuckDB
// nolint
var humanReadableSizeRegex = regexp.MustCompile(`^([\d.]+)\s*(\S+)$`)

// Reversed logic of StringUtil::BytesToHumanReadableString
// see https://github.com/cran/duckdb/blob/master/src/duckdb/src/common/string_util.cpp#L157
// Examples: 1 bytes, 2 bytes, 1KB, 1MB, 1TB, 1PB
// nolint
func humanReadableSizeToBytes(sizeStr string) (float64, error) {
	var multiplier float64

	match := humanReadableSizeRegex.FindStringSubmatch(sizeStr)

	if match == nil {
		return 0, fmt.Errorf("invalid size format: '%s'", sizeStr)
	}

	sizeFloat, err := strconv.ParseFloat(match[1], 64)
	if err != nil {
		return 0, err
	}

	switch match[2] {
	case "byte", "bytes":
		multiplier = 1
	case "KB":
		multiplier = 1000
	case "MB":
		multiplier = 1000 * 1000
	case "GB":
		multiplier = 1000 * 1000 * 1000
	case "TB":
		multiplier = 1000 * 1000 * 1000 * 1000
	case "PB":
		multiplier = 1000 * 1000 * 1000 * 1000 * 1000
	case "KiB":
		multiplier = 1024
	case "MiB":
		multiplier = 1024 * 1024
	case "GiB":
		multiplier = 1024 * 1024 * 1024
	case "TiB":
		multiplier = 1024 * 1024 * 1024 * 1024
	case "PiB":
		multiplier = 1024 * 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown size unit '%s' in '%s'", match[2], sizeStr)
	}

	return sizeFloat * multiplier, nil
}
