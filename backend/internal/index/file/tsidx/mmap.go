package tsidx

import "syscall"

// mmapReadOnly maps fd's first size bytes read-only.
func mmapReadOnly(fd, size int) ([]byte, error) {
	return syscall.Mmap(fd, 0, size, syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmap(data []byte) error {
	if data == nil {
		return nil
	}
	return syscall.Munmap(data)
}
