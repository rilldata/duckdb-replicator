package duckdbreplicator

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/marcboeker/go-duckdb"
	"go.uber.org/zap"
	"gocloud.dev/blob"
	"golang.org/x/sync/errgroup"
)

type CreateTableOptions struct {
	// View specifies whether the created table is a view.
	View bool
}

type Rows struct {
	*sql.Rows
	cleanUp func()
}

func (q *Rows) Close() error {
	q.cleanUp()
	return q.Rows.Close()
}

type DB interface {
	// Query executes a query that returns rows typically a SELECT.
	Query(ctx context.Context, query string, args ...any) (*Rows, error)

	// CreateTableAsSelect creates a new table by name from the results of the given SQL query.
	CreateTableAsSelect(ctx context.Context, name string, sql string, opts *CreateTableOptions) error

	// DropTable removes a table from the database.
	DropTable(ctx context.Context, name string) error

	// Sync synchronizes the database with the cloud storage.
	Sync(ctx context.Context) error
}

type DBOptions struct {
	// Clean specifies whether to start with a clean database or download data from cloud storage and start with backed up data.
	Clean bool
	// LocalPath is the path where local db files will be stored.
	LocalPath string

	// BucketHandle is the cloud storage bucket to use.
	BucketHandle *blob.Bucket
	// DBIdentifier is a unique identifier for the database. Files in the cloud storage bucket are stored in a directory named `DBIdenitifier`
	DBIdenitifier string

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
	LogLevel string
}

type BackupFormat string

const (
	BackupFormatUnknown BackupFormat = "unknown"
	BackupFormatDB      BackupFormat = "db"
	BackupFormatParquet BackupFormat = "parquet"
)

// NewDB creates a new DB instance.
// This can be a slow operation if the backed up database is large.
func NewDB(ctx context.Context, opts *DBOptions) (DB, error) {
	// TODO :: support clean run
	db := &db{
		opts:         opts,
		cloudStorage: blob.PrefixedBucket(opts.BucketHandle, opts.DBIdenitifier),
		logger:       zap.NewNop(),
		readPath:     filepath.Join(opts.LocalPath, opts.DBIdenitifier, "read"),
		writePath:    filepath.Join(opts.LocalPath, opts.DBIdenitifier, "write"),
	}
	err := db.downloadBackup(ctx)
	if err != nil {
		return nil, err
	}

	// create a read handle
	db.readHandle, err = db.openDBAndAttach(ctx, db.readPath, opts.ReadSettings)
	if err != nil {
		return nil, err
	}

	return db, nil
}

type db struct {
	opts *DBOptions

	readHandle   *sql.DB
	writeHandle  *sql.DB
	cloudStorage *blob.Bucket
	writeDirty   *atomic.Bool
	readDirty    *atomic.Bool

	writePath string
	readPath  string

	readMu  sync.RWMutex
	writeMu sync.Mutex

	logger *zap.Logger
}

var _ DB = &db{}

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

// DropTable implements DB.
func (d *db) DropTable(ctx context.Context, name string) error {
	panic("unimplemented")
}

func (d *db) CreateTableAsSelect(ctx context.Context, name string, sql string, opts *CreateTableOptions) error {
	d.logger.Debug("create table", zap.String("name", name), zap.Bool("view", opts.View))
	var err error
	d.writeHandle, err = d.openDBAndAttach(ctx, d.writePath, d.opts.WriteSettings)
	if err != nil {
		return err
	}
	// does this leak memory ?
	defer d.writeHandle.Close()

	if opts.View {
		version := fmt.Sprint(time.Now().UnixMilli())
		newVersionDir := filepath.Join(d.writePath, name, version)
		err := os.MkdirAll(newVersionDir, fs.ModePerm)
		if err != nil {
			return fmt.Errorf("create: unable to create dir %q: %w", name, err)
		}

		_, err = d.writeHandle.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS (%s\n)", safeSQLName(name), sql))
		if err != nil {
			_ = os.RemoveAll(newVersionDir)
			return fmt.Errorf("create: unable to create view: %w", err)
		}

		// write meta file
		err = writeMeta(newVersionDir, meta{ViewSQL: sql})
		if err != nil {
			_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DROP VIEW %s", safeSQLName(name)))
			_ = os.RemoveAll(newVersionDir)
			return err
		}

		// update version.txt
		err = os.WriteFile(filepath.Join(d.writePath, name, "version.txt"), []byte(version), fs.ModePerm)
		if err != nil {
			// drop view and remove dir
			_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DROP VIEW %s", safeSQLName(name)))
			_ = os.RemoveAll(filepath.Join(d.writePath, name, version))
			return fmt.Errorf("create: unable to write version file: %w", err)
		}

		// copy to backup location
		err = d.replicate(ctx, name)
		if err != nil {
			// drop view and remove dir
			_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DROP VIEW %s", safeSQLName(name)))
			_ = os.RemoveAll(filepath.Join(d.writePath, name, version))
			return fmt.Errorf("create: unable to replicate view: %w", err)
		}
		return d.Sync(ctx)
	}

	// check if some older version exists
	oldVersion, oldVersionExists, _ := tableVersion(d.writePath, name)

	newVersion := strconv.FormatInt(time.Now().UnixMilli(), 10)
	// create new version directory
	newVersionDir := filepath.Join(d.writePath, name, newVersion)
	err = os.MkdirAll(newVersionDir, fs.ModePerm)
	if err != nil {
		return fmt.Errorf("create: unable to create dir %q: %w", name, err)
	}

	// create db file
	dbFile := filepath.Join(newVersionDir, "data.db")
	dbName := safeSQLName(fmt.Sprintf("%s__db__data", name))

	// detach existing db
	_, err = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE IF EXISTS %s", dbName), nil)
	if err != nil {
		_ = os.RemoveAll(newVersionDir)
		return fmt.Errorf("create: detach %q db failed: %w", dbName, err)
	}

	// attach new db
	_, err = d.writeHandle.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s", safeSQLString(dbFile), dbName), nil)
	if err != nil {
		_ = os.RemoveAll(newVersionDir)
		return fmt.Errorf("create: attach %q db failed: %w", dbFile, err)
	}

	// ingest data
	_, err = d.writeHandle.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE TABLE %s.default AS (%s\n)", safeSQLName(dbName), sql), nil)
	if err != nil {
		_ = os.RemoveAll(newVersionDir)
		_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", dbName))
		return fmt.Errorf("create: create %q.default table failed: %w", dbName, err)
	}

	// write meta file
	err = writeMeta(newVersionDir, meta{Format: string(BackupFormatDB)})
	if err != nil {
		_ = os.RemoveAll(newVersionDir)
		_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", dbName))
		return err
	}

	// update version.txt
	err = os.WriteFile(filepath.Join(d.writePath, name, "version.txt"), []byte(newVersion), fs.ModePerm)
	if err != nil {
		_ = os.RemoveAll(newVersionDir)
		_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", dbName))
		return fmt.Errorf("create: write version file failed: %w", err)
	}

	// at this point write is ahead of backup

	qry, err := d.generateSelectQuery(ctx, d.writeHandle, dbName)
	if err != nil {
		// revert to previous version
		err := os.WriteFile(filepath.Join(d.writePath, name, "version.txt"), []byte(oldVersion), fs.ModePerm)
		if err != nil {
			d.writeDirty.Store(true)
		}
		_ = os.RemoveAll(newVersionDir)
		_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", dbName))
		return err
	}

	// create view query
	_, err = d.writeHandle.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS %s", safeSQLName(name), qry))
	if err != nil {
		// revert to previous version
		err := os.WriteFile(filepath.Join(d.writePath, name, "version.txt"), []byte(oldVersion), fs.ModePerm)
		if err != nil {
			d.writeDirty.Store(true)
		}
		_ = os.RemoveAll(newVersionDir)
		_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", dbName))
		return fmt.Errorf("create: create view %q failed: %w", name, err)
	}

	if err := d.replicate(ctx, name); err != nil {
		// revert to previous version
		err := os.WriteFile(filepath.Join(d.writePath, name, "version.txt"), []byte(oldVersion), fs.ModePerm)
		if err != nil {
			d.writeDirty.Store(true)
		}
		_ = os.RemoveAll(newVersionDir)
		_, _ = d.writeHandle.ExecContext(ctx, fmt.Sprintf("DETACH DATABASE %s", dbName))
		return fmt.Errorf("create: replicate failed: %w", err)
	}

	if oldVersionExists {
		_ = os.Remove(filepath.Join(d.writePath, name, oldVersion))
		// also delete from the backup location
	}
	return d.Sync(ctx)
}

// Sync implements DB.
// Table is optional. If table is provided, only that table is synced.
// Otherwise, the entire database is synced.
func (d *db) Sync(ctx context.Context) error {
	readPath, err := os.MkdirTemp(d.opts.LocalPath, "read")
	if err != nil {
		return err
	}

	err = copyDir(d.writePath, readPath)
	if err != nil {
		return err
	}

	handle, err := d.openDBAndAttach(ctx, readPath, d.opts.ReadSettings)
	if err != nil {
		_ = os.RemoveAll(readPath)
		return err
	}

	var (
		oldReadPath string
		oldDBHandle *sql.DB
	)
	d.readMu.Lock()
	// swap read handle
	oldDBHandle = d.readHandle
	d.readHandle = handle
	// swap read path
	oldReadPath = d.readPath
	d.readPath = readPath
	d.readMu.Unlock()

	// close old read handle
	err = oldDBHandle.Close()
	if err != nil {
		d.logger.Error("error in closing old read handle", zap.Error(err))
	}
	// remove old read path
	_ = os.RemoveAll(oldReadPath)
	return nil
}

func (d *db) downloadBackup(ctx context.Context) error {
	// Create an errgroup for background downloads with limited concurrency.
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	objects := d.cloudStorage.List(nil)

	for {
		// Stop the loop if the ctx was cancelled
		var stop bool
		select {
		case <-ctx.Done():
			stop = true
		default:
			// don't break
		}
		if stop {
			break // can't use break inside the select
		}

		g.Go(func() error {
			// Create a path that maintains the same relative path as in the bucket
			obj, err := objects.Next(ctx)
			if err != nil {
				return err
			}

			filename := filepath.Join(d.writePath, obj.Key)
			if err := os.MkdirAll(filepath.Dir(filename), os.ModePerm); err != nil {
				return err
			}

			return retry(5, 10*time.Second, func() error {
				// TODO :: Handle truncate of the file
				file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, os.ModePerm)
				if err != nil {
					return err
				}
				defer file.Close()

				rdr, err := d.cloudStorage.NewReader(ctx, obj.Key, nil)
				if err != nil {
					return err
				}
				defer rdr.Close()

				_, err = io.Copy(file, rdr)
				return err
			})
		})
	}

	// Wait for all outstanding downloads to complete
	return g.Wait()
}

func (d *db) replicate(ctx context.Context, table string) error {
	version, exist, err := tableVersion(d.writePath, table)
	if err != nil {
		return err
	}

	if !exist {
		return fmt.Errorf("table %q not found", table)
	}

	// upload version directory and copy to read location
	return retry(5, 10*time.Second, func() error {
		// Nested directories ??
		path := filepath.Join(d.writePath, table, version)
		entries, err := os.ReadDir(path)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			// nested directories
			if !entry.IsDir() {
				continue
			}

			wr, err := os.Open(filepath.Join(path, entry.Name()))
			if err != nil {
				return err
			}

			// upload to cloud storage
			err = d.cloudStorage.Upload(ctx, filepath.Join(table, version, entry.Name()), wr, nil)
			if err != nil {
				wr.Close()
				return err
			}
			wr.Close()
		}

		// update version.txt

		return d.cloudStorage.WriteAll(ctx, filepath.Join(table, "version.txt"), []byte(version), nil)
	})
}

func (d *db) openDBAndAttach(ctx context.Context, path string, settings map[string]string) (*sql.DB, error) {
	// open the db
	url, err := url.Parse(filepath.Join(path, "stage.db"))
	if err != nil {
		return nil, err
	}
	query := url.Query()
	for k, v := range settings {
		query.Set(k, v)
	}
	url.RawQuery = query.Encode()
	connector, err := duckdb.NewConnector(url.String(), func(execer driver.ExecerContext) error {
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

	err = d.attachDBs(ctx, db, path)
	if err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func (d *db) attachDBs(ctx context.Context, db *sql.DB, path string) error {
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
			d.logger.Error("error in fetching db version", zap.String("table", entry.Name()), zap.Error(err))
			_ = os.RemoveAll(path)
			continue
		}
		if !exist {
			_ = os.RemoveAll(path)
			continue
		}
		path := filepath.Join(path, version)

		// read meta file
		f, err := os.ReadFile(filepath.Join(path, "meta.json"))
		if err != nil {
			_ = os.RemoveAll(path)
			d.logger.Error("error in reading meta file", zap.String("table", entry.Name()), zap.Error(err))
			// May be keep it as a config to return error or continue ?
			continue
		}
		var meta meta
		err = json.Unmarshal(f, &meta)
		if err != nil {
			_ = os.RemoveAll(path)
			d.logger.Error("error in unmarshalling meta file", zap.String("table", entry.Name()), zap.Error(err))
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
			dbName := safeSQLName(fmt.Sprintf("%s__db__data", entry.Name()))
			_, err := db.ExecContext(ctx, fmt.Sprintf("ATTACH %s AS %s", safeSQLString(filepath.Join(path, "data.db")), dbName))
			if err != nil {
				d.logger.Error("error in attaching db", zap.String("table", entry.Name()), zap.Error(err))
				_ = os.RemoveAll(filepath.Join(path))
				continue
			}
			_, err = db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE VIEW %s AS SELECT * FROM %s", safeSQLName(entry.Name()), dbName))
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

func (d *db) generateSelectQuery(ctx context.Context, handle *sql.DB, db string) (string, error) {
	if !d.opts.StableSelect {
		return fmt.Sprintf("SELECT * FROM %s.default", safeSQLName(db)), nil
	}
	rows, err := handle.QueryContext(ctx, `
			SELECT column_name AS name
			FROM information_schema.columns
			WHERE table_catalog = %s AND table_name = 'default'
			ORDER BY name ASC`, safeSQLString(db))
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

	return fmt.Sprintf("SELECT %s FROM %s.default", strings.Join(cols, ", "), safeSQLName(db)), nil
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

type meta struct {
	ViewSQL string
	Format  string
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
