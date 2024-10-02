package main

import (
	"context"
	"fmt"
	"io"

	"gocloud.dev/blob"
	_ "gocloud.dev/blob/s3blob"
)

func main() {
	bucket, err := blob.OpenBucket(context.Background(), "s3://rill-developer.rilldata.io")
	if err != nil {
		panic(err)
	}
	defer bucket.Close()

	bucket = blob.PrefixedBucket(bucket, "trips/")

	ctx := context.Background()
	iter := bucket.List(&blob.ListOptions{Prefix: "year=2023/month=6/"})
	for {
		obj, err := iter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				fmt.Println("EOF")
				break
			}
			panic(err)
		}
		fmt.Println(obj.Key)
		err = bucket.Delete(ctx, obj.Key)
		if err != nil {
			panic(err)
		}
	}
	return
}
