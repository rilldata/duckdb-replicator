package duckdbreplicator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"golang.org/x/sync/errgroup"
)

type BackupFormat string

const (
	BackupFormatUnknown BackupFormat = "unknown"
	BackupFormatDB      BackupFormat = "db"
	BackupFormatParquet BackupFormat = "parquet"
)

type BackupProvider struct {
	bucket *blob.Bucket
}

func (b *BackupProvider) Close() error {
	return b.bucket.Close()
}

type GCSBackupProviderOptions struct {
	// UseHostCredentials specifies whether to use the host's default credentials.
	UseHostCredentials         bool
	ApplicationCredentialsJSON string
	// Bucket is the GCS bucket to use for backups. Should be of the form `bucket-name`.
	Bucket string
	// BackupFormat specifies the format of the backup.
	// TODO :: implement backup format. Fixed to DuckDB for now.
	BackupFormat BackupFormat
	// UnqiueIdentifier is used to store backups in a unique location.
	// This must be set when multiple databases are writing to the same bucket.
	UniqueIdentifier string
}

// NewGCSBackupProvider creates a new BackupProvider based on GCS.
func NewGCSBackupProvider(ctx context.Context, opts *GCSBackupProviderOptions) (*BackupProvider, error) {
	client, err := newClient(ctx, opts.ApplicationCredentialsJSON, opts.UseHostCredentials)
	if err != nil {
		return nil, err
	}

	bucket, err := gcsblob.OpenBucket(ctx, client, opts.Bucket, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to open bucket %q, %w", opts.Bucket, err)
	}

	if opts.UniqueIdentifier != "" {
		if !strings.HasSuffix(opts.UniqueIdentifier, "/") {
			opts.UniqueIdentifier += "/"
		}
		bucket = blob.PrefixedBucket(bucket, opts.UniqueIdentifier)
	}
	return &BackupProvider{
		bucket: bucket,
	}, nil
}

// syncWrite syncs the write path with the backup location.
func (d *db) syncWrite(ctx context.Context) error {
	if !d.writeDirty || d.backup == nil {
		// optimisation to skip sync if write was already synced
		return nil
	}
	d.logger.Info("syncing from backup")
	// Create an errgroup for background downloads with limited concurrency.
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(8)

	objects := d.backup.List(&blob.ListOptions{
		Delimiter: "/", // only list directories with a trailing slash and IsDir set to true
	})

	tblVersions := make(map[string]string)
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

		obj, err := objects.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if !obj.IsDir {
			continue
		}

		table := strings.TrimSuffix(obj.Key, "/")
		d.logger.Info("SyncWithObjectStorage: discovered table", slog.String("table", table))

		// get version of the table
		res, err := d.backup.ReadAll(ctx, filepath.Join(table, "version.txt"))
		if err != nil {
			// invalid table directory
			// TODO :: differ between not found and other errors
			d.logger.Warn("SyncWithObjectStorage: invalid table directory", slog.String("table", table))
			_ = d.deleteBackup(ctx, table, "")
			continue
		}
		backedUpVersion := string(res)
		tblVersions[table] = backedUpVersion

		// check with current version
		version, exists, _ := tableVersion(d.writePath, table)
		if exists && version == backedUpVersion {
			d.logger.Info("SyncWithObjectStorage: table is already up to date", slog.String("table", table))
			continue
		}

		tableDir := filepath.Join(d.writePath, table)
		// truncate existing table directory
		if err := os.RemoveAll(tableDir); err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Join(tableDir, backedUpVersion), os.ModePerm); err != nil {
			return err
		}

		tblIter := d.backup.List(&blob.ListOptions{Prefix: filepath.Join(table, backedUpVersion)})
		// download all objects in the table and current version
		for {
			obj, err := tblIter.Next(ctx)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return err
			}
			g.Go(func() error {
				return retry(5, 10*time.Second, func() error {
					file, err := os.Create(filepath.Join(d.writePath, obj.Key))
					if err != nil {
						return err
					}
					defer file.Close()

					rdr, err := d.backup.NewReader(ctx, obj.Key, nil)
					if err != nil {
						return err
					}
					defer rdr.Close()

					_, err = io.Copy(file, rdr)
					return err
				})
			})
		}
	}

	// Wait for all outstanding downloads to complete
	err := g.Wait()
	if err != nil {
		return err
	}

	// Update table versions
	for table, version := range tblVersions {
		err = os.WriteFile(filepath.Join(d.writePath, table, "version.txt"), []byte(version), fs.ModePerm)
		if err != nil {
			return err
		}
	}

	// remove any tables that are not in backup
	entries, err := os.ReadDir(d.writePath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		if _, ok := tblVersions[entry.Name()]; ok {
			continue
		}
		err = os.RemoveAll(filepath.Join(d.writePath, entry.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func (d *db) syncBackup(ctx context.Context, table string) error {
	if d.backup == nil {
		return nil
	}
	d.logger.Info("syncing table", slog.String("table", table))
	version, exist, err := tableVersion(d.writePath, table)
	if err != nil {
		return err
	}

	if !exist {
		return fmt.Errorf("table %q not found", table)
	}

	path := filepath.Join(d.writePath, table, version)
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}

	for _, entry := range entries {
		d.logger.Info("replicating file", slog.String("file", entry.Name()), slog.String("path", path))
		// no directory should exist as of now
		if entry.IsDir() {
			d.logger.Info("found directory in path which should not exist", slog.String("file", entry.Name()), slog.String("path", path))
			continue
		}

		wr, err := os.Open(filepath.Join(path, entry.Name()))
		if err != nil {
			return err
		}

		// upload to cloud storage
		err = retry(5, 10*time.Second, func() error {
			return d.backup.Upload(ctx, filepath.Join(table, version, entry.Name()), wr, &blob.WriterOptions{
				ContentType: "application/octet-stream",
			})
		})
		wr.Close()
		if err != nil {
			return err
		}
	}

	// update version.txt
	return d.backup.WriteAll(ctx, filepath.Join(table, "version.txt"), []byte(version), nil)
}

// deleteBackup deletes backup.
// If table is specified, only that table is deleted.
// If table and version is specified, only that version of the table is deleted.
func (d *db) deleteBackup(ctx context.Context, table, version string) error {
	if d.backup == nil {
		return nil
	}
	if table == "" && version != "" {
		return fmt.Errorf("table must be specified if version is specified")
	}
	var prefix string
	if table != "" {
		if version != "" {
			prefix = filepath.Join(table, version) + "/"
		} else {
			prefix = table + "/"
		}
	}

	iter := d.backup.List(&blob.ListOptions{Prefix: prefix})
	for {
		obj, err := iter.Next(ctx)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		err = retry(5, 10*time.Second, func() error { return d.backup.Delete(ctx, obj.Key) })
		if err != nil {
			return err
		}
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
