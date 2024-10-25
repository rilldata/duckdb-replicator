package main

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	duckdbreplicator "github.com/rilldata/duckdb-replicator"
	_ "gocloud.dev/blob/gcsblob"
)

// func main() {
// 	bucket, err := blob.OpenBucket(context.Background(), "gs://anshul-rill-test")
// 	if err != nil {
// 		panic(err)
// 	}
// 	defer bucket.Close()

// 	bucket = blob.PrefixedBucket(bucket, "replicator-test/")

// 	ctx := context.Background()
// 	iter := bucket.List(&blob.ListOptions{Prefix: "test-2"})
// 	for {
// 		obj, err := iter.Next(ctx)
// 		if err != nil {
// 			if err == io.EOF {
// 				fmt.Println("EOF")
// 				break
// 			}
// 			panic(err)
// 		}
// 		fmt.Println(obj.Key + " " + strconv.FormatBool(obj.IsDir))
// 		// err = bucket.Delete(ctx, obj.Key)
// 		// if err != nil {
// 		// 	panic(err)
// 		// }
// 	}
// }

func main() {
	// bucket, err := blob.OpenBucket(context.Background(), "gs://anshul-rill-test")
	// if err != nil {
	// 	panic(err)
	// }
	// bucket = blob.PrefixedBucket(bucket, "replicator-test/")

	dbOptions := &duckdbreplicator.DBOptions{
		LocalPath:      "/home/anshul/workspace/duckdb-replicator-test",
		BackupProvider: nil,
		BackupFormat:   duckdbreplicator.BackupFormatDB,
		ReadSettings:   map[string]string{"memory_limit": "2GB", "threads": "1"},
		WriteSettings:  map[string]string{"memory_limit": "8GB", "threads": "4"},
		InitQueries:    []string{"SET autoinstall_known_extensions=true", "SET autoload_known_extensions=true"},
		StableSelect:   true,
		Logger:         slog.Default(),
	}

	db, err := duckdbreplicator.NewDB(context.Background(), "756c6367-e807-43ff-8b07-df1bae29c57e", dbOptions)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	t := time.Now()
	err = db.CreateTableAsSelect(context.Background(), "test-2", `SELECT * FROM read_parquet('/home/anshul/Downloads/trip_data/yellow*.parquet') LIMIT 1000`, nil)
	if err != nil {
		panic(err)
	}
	fmt.Printf("time taken %v\n", time.Since(t))

	err = db.RenameTable(context.Background(), "test-2", "test")
	if err != nil {
		panic(err)
	}

	err = db.InsertTableAsSelect(context.Background(), "test", `SELECT * FROM read_parquet('/home/anshul/Downloads/trip_data/yellow*.parquet') LIMIT 1000`, &duckdbreplicator.InsertTableOptions{
		Strategy: duckdbreplicator.IncrementalStrategyAppend,
	})
	if err != nil {
		panic(err)
	}

	t = time.Now()
	rows, err := db.Query(context.Background(), `SELECT count(*) FROM "test"`)
	if err != nil {
		fmt.Printf("error %v\n", err)
	}
	defer rows.Close()
	fmt.Printf("time taken %v\n", time.Since(t))

	var count int
	for rows.Next() {
		err = rows.Scan(&count)
		if err != nil {
			panic(err)
		}
		fmt.Println(count)
	}

}
