package glcb

import (
	"crypto/sha256"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ReadBlobDigest returns the whole-blob digest recorded in the TOC footer of
// the GLCB at path. It reads the footer only, so it states what the blob
// claims, not what its bytes hash to; VerifyBlobDigest checks the bytes.
func ReadBlobDigest(path string) ([32]byte, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return [32]byte{}, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return [32]byte{}, err
	}
	toc, err := ReadTOC(f, info.Size())
	if err != nil {
		return [32]byte{}, err
	}
	return toc.BlobDigest, nil
}

// VerifyBlobDigest hashes every byte of the GLCB at path before its TOC
// footer and returns that digest when it equals the one the footer records.
// A blob whose bytes were altered after it was written — however well-formed
// the result — fails here, where reading the footer alone would accept it.
func VerifyBlobDigest(path string) ([32]byte, error) {
	f, err := os.Open(filepath.Clean(path))
	if err != nil {
		return [32]byte{}, err
	}
	defer func() { _ = f.Close() }()
	info, err := f.Stat()
	if err != nil {
		return [32]byte{}, err
	}
	toc, err := ReadTOC(f, info.Size())
	if err != nil {
		return [32]byte{}, err
	}
	h := sha256.New()
	if _, err := io.Copy(h, io.NewSectionReader(f, 0, info.Size()-int64(tocFooterSize))); err != nil {
		return [32]byte{}, fmt.Errorf("hash blob: %w", err)
	}
	var sum [32]byte
	copy(sum[:], h.Sum(nil))
	if sum != toc.BlobDigest {
		return [32]byte{}, fmt.Errorf("blob bytes hash to %x but the TOC footer records %x", sum[:8], toc.BlobDigest[:8])
	}
	return sum, nil
}
