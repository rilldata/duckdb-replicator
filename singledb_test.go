package duckdbreplicator

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSingleDB_test(t *testing.T) {
	ctx := context.Background()
	db, err := NewSingleDB(ctx, &SingleDBOptions{
		DSN: "",
	})
	require.NoError(t, err)

	// create table
	err = db.CreateTableAsSelect(ctx, "test-2", "SELECT 1 AS id, 'India' AS country", nil)
	require.NoError(t, err)

	// rename table
	err = db.RenameTable(ctx, "test-2", "test")
	require.NoError(t, err)

	// insert into table
	err = db.InsertTableAsSelect(ctx, "test", "SELECT 2 AS id, 'USA' AS country", nil)
	require.NoError(t, err)

	// add column
	err = db.AddTableColumn(ctx, "test", "currency_score", "INT")
	require.NoError(t, err)

	// alter column
	err = db.AlterTableColumn(ctx, "test", "currency_score", "FLOAT")
	require.NoError(t, err)

	// select from table
	conn, release, err := db.AcquireReadConnection(ctx)
	require.NoError(t, err)

	var (
		id            int
		country       string
		currencyScore sql.NullFloat64
	)

	err = conn.Connx().QueryRowxContext(ctx, "SELECT id, country, currency_score FROM test WHERE id = 2").Scan(&id, &country, &currencyScore)
	require.NoError(t, err)
	require.Equal(t, 2, id)
	require.Equal(t, "USA", country)
	require.Equal(t, false, currencyScore.Valid)

	err = release()
	require.NoError(t, err)

	// drop table
	err = db.DropTable(ctx, "test")
	require.NoError(t, err)
}
