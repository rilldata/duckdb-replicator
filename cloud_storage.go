package duckdbreplicator

import (
	"context"
	"io"

	"gocloud.dev/blob"
)

func (d *db) deleteFolder(ctx context.Context, name string) error {
	iter := d.cloudStorage.List(&blob.ListOptions{Prefix: name + "/"})
	for {
		obj, err := iter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		err = d.cloudStorage.Delete(ctx, obj.Key)
		if err != nil {
			return err
		}
	}
	return nil
}
