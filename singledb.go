package duckdbreplicator

import (
	"context"
	"database/sql/driver"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"strings"

	"github.com/XSAM/otelsql"
	"github.com/jmoiron/sqlx"
	"github.com/marcboeker/go-duckdb"
	"go.opentelemetry.io/otel/attribute"
)

type singledb struct {
	db     *sqlx.DB
	logger *slog.Logger
}

type SingleDBOptions struct {
	DSN         string
	Clean       bool
	InitQueries []string
	Logger      *slog.Logger
}

// NewSingleDB creates a new DB that writes to and reads from a single DuckDB database.
// This is useful for testing and small datasets.
func NewSingleDB(ctx context.Context, opts *SingleDBOptions) (DB, error) {
	if opts.Clean {
		u, err := url.Parse(opts.DSN)
		if err != nil {
			return nil, err
		}
		if u.Path != "" {
			err = os.Remove(u.Path)
			if err != nil && !os.IsNotExist(err) {
				return nil, err
			}
		}
	}
	connector, err := duckdb.NewConnector(opts.DSN, func(execer driver.ExecerContext) error {
		for _, qry := range opts.InitQueries {
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

	err = otelsql.RegisterDBStatsMetrics(db.DB, otelsql.WithAttributes(attribute.String("db.system", "duckdb")))
	if err != nil {
		return nil, fmt.Errorf("registering db stats metrics: %w", err)
	}

	err = db.PingContext(context.Background())
	if err != nil {
		db.Close()
		return nil, err
	}
	return &singledb{
		db:     db,
		logger: opts.Logger,
	}, nil
}

// AcquireReadConnection implements DB.
func (s *singledb) AcquireReadConnection(ctx context.Context) (conn *sqlx.Conn, release func() error, err error) {
	conn, err = s.db.Connx(ctx)
	if err != nil {
		return nil, nil, err
	}

	return conn, conn.Close, nil
}

// AddTableColumn implements DB.
func (s *singledb) AddTableColumn(ctx context.Context, tableName string, columnName string, typ string) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", safeSQLString(tableName), safeSQLName(columnName), typ))
	return err
}

// AlterTableColumn implements DB.
func (s *singledb) AlterTableColumn(ctx context.Context, tableName string, columnName string, newType string) error {
	_, err := s.db.ExecContext(ctx, fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", safeSQLName(tableName), safeSQLName(columnName), newType))
	return err
}

// Close implements DB.
func (s *singledb) Close() error {
	return s.db.Close()
}

// CreateTableAsSelect implements DB.
func (s *singledb) CreateTableAsSelect(ctx context.Context, name string, sql string, opts *CreateTableOptions) error {
	var typ string
	if opts != nil && opts.View {
		typ = "VIEW"
	} else {
		typ = "TABLE"
	}

	_, err := s.db.ExecContext(ctx, fmt.Sprintf("CREATE OR REPLACE %s %s AS (%s\n)", typ, safeSQLName(name), sql))
	return err
}

// DropTable implements DB.
func (s *singledb) DropTable(ctx context.Context, name string) error {
	view, err := s.isView(ctx, name)
	if err != nil {
		return err
	}
	var typ string
	if view {
		typ = "VIEW"
	} else {
		typ = "TABLE"
	}

	_, err = s.db.ExecContext(ctx, fmt.Sprintf("DROP %s %s", typ, safeSQLName(name)))
	return err
}

// InsertTableAsSelect implements DB.
func (s *singledb) InsertTableAsSelect(ctx context.Context, name string, sql string, opts *InsertTableOptions) error {
	if opts == nil {
		opts = &InsertTableOptions{
			Strategy: IncrementalStrategyAppend,
		}
	}

	return execIncrementalInsert(ctx, s.db, name, sql, opts)
}

// Query implements DB.
func (s *singledb) Query(ctx context.Context, query string, args ...any) (res *sqlx.Rows, release func() error, err error) {
	res, err = s.db.QueryxContext(ctx, query, args...)
	if err != nil {
		return nil, nil, err
	}

	return res, res.Close, nil
}

// RenameTable implements DB.
func (s *singledb) RenameTable(ctx context.Context, oldName string, newName string) error {
	view, err := s.isView(ctx, oldName)
	if err != nil {
		return err
	}
	var typ string
	if view {
		typ = "VIEW"
	} else {
		typ = "TABLE"
	}

	_, err = s.db.ExecContext(ctx, fmt.Sprintf("ALTER %s %s RENAME TO %s", typ, safeSQLName(oldName), safeSQLName(newName)))
	return err
}

// Sync implements DB.
func (s *singledb) Sync(ctx context.Context) error {
	return nil
}

func (s *singledb) isView(ctx context.Context, name string) (bool, error) {
	var view bool
	err := s.db.QueryRowxContext(ctx, `
		SELECT 
			UPPER(table_type) = 'VIEW' 
		FROM 
			information_schema.tables 
		WHERE 
			table_catalog = current_database() 
			AND table_schema = 'main' 
			AND LOWER(table_name) = LOWER(?)
	`, name).Scan(&view)
	if err != nil {
		return false, err
	}
	return view, nil
}

var _ DB = &singledb{}
