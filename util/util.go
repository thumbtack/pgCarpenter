package util

import (
	"bufio"
	"io"
	"io/ioutil"
	"os"

	"github.com/pierrec/lz4"
	"go.uber.org/zap"
)

// MustRemoveFile tries to delete the file path from the local file system. On error a message is logged.
func MustRemoveFile(path string, logger *zap.Logger) {
	logger.Debug("Removing file", zap.String("path", path))
	if err := os.Remove(path); err != nil {
		// there's not a lot we can do here
		logger.Error("Failed to remove file", zap.String("path", path), zap.Error(err))
	}
}

// IsCompressed returns true iff the file is compressed, i.e., .lz4 extension
func IsCompressed(path string) bool {
	return path[len(path)-len(lz4.Extension):] == lz4.Extension
}

// Compress compresses the file inPath using tmpDir fo storing the compressed output file and
// any intermediate temporary files it might need to create. It returns the full path to the
// compressed file, or an error.
func Compress(inPath string, tmpDir string) (string, error) {
	// create a temporary file with a unique name compress it -- multiple files
	// are named 000: pg_notify/0000, pg_subtrans/0000
	outFile, err := ioutil.TempFile(tmpDir, "pgCarpenter.")
	if err != nil {
		return "", err
	}

	// open input file
	inFile, err := os.Open(inPath)
	if err != nil {
		return "", err
	}
	// we open this for read only, and this process exists after a finite (short)
	// period of time; there's no need to throw an error if closing it fails
	defer inFile.Close()

	// buffer read from the input file and lz4 writer
	r := bufio.NewReader(inFile)
	w := lz4.NewWriter(outFile)

	// read 4k at a time
	buf := make([]byte, 4096)
	for {
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return "", err
		}

		// we're done
		if n == 0 {
			break
		}

		// write the 4k chunk
		if _, err := w.Write(buf[:n]); err != nil {
			return "", err
		}
	}

	// flush any pending compressed data
	if err = w.Flush(); err != nil {
		return "", err
	}

	// make sure we successfully close the compressed file
	if err := outFile.Close(); err != nil {
		return "", err
	}

	return outFile.Name(), nil
}

// Decompress decompresses the file inPath to outPath.
func Decompress(inPath string, outPath string) error {
	// open the input, compressed file
	inFile, err := os.Open(inPath)
	if err != nil {
		return err
	}
	// we open this for read only, and this process exists after a finite (short)
	// period of time; there's no need to throw an error if closing it fails
	defer inFile.Close()

	// open output file
	outFile, err := os.Create(outPath)
	if err != nil {
		return err
	}

	// lz4 read buffer
	r := lz4.NewReader(inFile)
	// write buffer
	w := bufio.NewWriter(outFile)

	// 4kb chunks
	buf := make([]byte, 4096)
	for {
		// read a chunk
		n, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}

		// write a chunk
		if _, err := w.Write(buf[:n]); err != nil {
			return err
		}
	}

	// flush and pending data
	if err = w.Flush(); err != nil {
		panic(err)
	}

	// make sure we successfully close the compressed file
	if err := outFile.Close(); err != nil {
		return err
	}

	return nil
}
