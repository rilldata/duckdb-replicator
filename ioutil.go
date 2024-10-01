package duckdbreplicator

import (
	"io"
	"os"
	"path/filepath"
)

func copyDir(dst, src string) error {
	// Create the destination directory
	err := os.MkdirAll(dst, os.ModePerm)
	if err != nil {
		return err
	}

	// Read the contents of the source directory
	entries, err := os.ReadDir(src)
	if err != nil {
		return err
	}

	// Copy the contents of the source directory
	for _, entry := range entries {
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		if entry.IsDir() {
			err = copyDir(dstPath, srcPath)
			if err != nil {
				return err
			}
		} else {
			err = copyFile(dstPath, srcPath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func copyFile(dst string, src string) error {
	// Open the source file
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	// Create the destination file
	dstFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer dstFile.Close()

	// Copy the content from source to destination
	_, err = io.Copy(dstFile, srcFile)
	if err != nil {
		return err
	}
	return nil
}
