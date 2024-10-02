package duckdbreplicator

import (
	"io"
	"os"
	"path/filepath"
)

func syncDir(dst, src string) error {
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

	srcFolders := make(map[string]any)
	// Copy the contents of the source directory
	for _, entry := range entries {
		srcFolders[entry.Name()] = nil
		srcPath := filepath.Join(src, entry.Name())
		dstPath := filepath.Join(dst, entry.Name())

		// Check if there is already a table with a version
		srcVersion, srcExist, _ := tableVersion(srcPath, entry.Name())
		destVersion, destExist, _ := tableVersion(dstPath, entry.Name())
		if srcExist && destExist && srcVersion == destVersion {
			continue
		}

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

		// Remove the old destination version of the table
		if destExist {
			err = os.RemoveAll(filepath.Join(dstPath, destVersion))
			if err != nil {
				return err
			}
		}
	}

	// iterate over the destination folder and remove any table that is not in the source folder
	entries, err = os.ReadDir(dst)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		_, ok := srcFolders[entry.Name()]
		if ok {
			continue
		}
		err = os.RemoveAll(filepath.Join(dst, entry.Name()))
		if err != nil {
			return err
		}
	}
	return nil
}

func copyDir(dst, src string) error {
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
