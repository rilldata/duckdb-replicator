package duckdbreplicator_test

import (
	"context"
	"io"
	"log/slog"
	"testing"

	duckdbreplicator "github.com/rilldata/duckdb-replicator"
	"github.com/stretchr/testify/require"
)

func TestDB(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()
	db, err := duckdbreplicator.NewDB(ctx, "test", &duckdbreplicator.DBOptions{
		LocalPath:      dir,
		BackupProvider: nil,
		ReadSettings:   map[string]string{"memory_limit": "2GB", "threads": "1"},
		WriteSettings:  map[string]string{"memory_limit": "2GB", "threads": "1"},
		InitQueries:    []string{"SET autoinstall_known_extensions=true", "SET autoload_known_extensions=true"},
		StableSelect:   true,
		Logger:         slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	require.NoError(t, err)

	// create table
	err = db.CreateTableAsSelect(ctx, "test", "SELECT 1 AS id, 'India' AS country", nil)
	require.NoError(t, err)

	// query table
	rows, err := db.Query(ctx, "SELECT id, country FROM test")
	require.NoError(t, err)

	var (
		id      int
		country string
	)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &country))
	require.Equal(t, 1, id)
	require.Equal(t, "India", country)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// rename table
	err = db.RenameTable(ctx, "test", "test2")
	require.NoError(t, err)

	// drop old table
	err = db.DropTable(ctx, "test")
	require.Error(t, err)

	// insert into table
	err = db.InsertTableAsSelect(ctx, "test2", "SELECT 2 AS id, 'US' AS country", nil)
	require.NoError(t, err)

	// merge into table
	err = db.InsertTableAsSelect(ctx, "test2", "SELECT 2 AS id, 'USA' AS country", &duckdbreplicator.InsertTableOptions{
		Strategy:  duckdbreplicator.IncrementalStrategyMerge,
		UniqueKey: []string{"id"},
	})
	require.NoError(t, err)

	// query table
	rows, err = db.Query(ctx, "SELECT id, country FROM test2 where id = 2")
	require.NoError(t, err)
	require.True(t, rows.Next())
	require.NoError(t, rows.Scan(&id, &country))
	require.Equal(t, 2, id)
	require.Equal(t, "USA", country)
	require.False(t, rows.Next())
	require.NoError(t, rows.Err())
	require.NoError(t, rows.Close())

	// Add column
	err = db.AddTableColumn(ctx, "test2", "city", "TEXT")
	require.NoError(t, err)

	// drop table
	err = db.DropTable(ctx, "test2")
	require.NoError(t, err)
}
