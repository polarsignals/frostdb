package vfs

import (
	experimentalsys "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/sys"
)

type file struct {
	// internal is the underlying file system to delegate to. This is
	// purposefully not embedded so that any new methods need to be explicitly
	// added.
	internal experimentalsys.File
}

var _ experimentalsys.File = (*file)(nil)

func newFile(f experimentalsys.File) *file {
	return &file{internal: f}
}

func (f *file) Dev() (uint64, experimentalsys.Errno) {
	if isShutdown {
		return 0, experimentalsys.EIO
	}
	return f.internal.Dev()
}

func (f *file) Ino() (sys.Inode, experimentalsys.Errno) {
	if isShutdown {
		return 0, experimentalsys.EIO
	}
	return f.internal.Ino()
}

func (f *file) IsDir() (bool, experimentalsys.Errno) {
	if isShutdown {
		return false, experimentalsys.EIO
	}
	return f.internal.IsDir()
}

func (f *file) IsAppend() bool {
	return f.internal.IsAppend()
}

func (f *file) SetAppend(enable bool) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return f.internal.SetAppend(enable)
}

func (f *file) Stat() (sys.Stat_t, experimentalsys.Errno) {
	if isShutdown {
		return sys.Stat_t{}, experimentalsys.EIO
	}
	return f.internal.Stat()
}

func (f *file) Read(buf []byte) (n int, errno experimentalsys.Errno) {
	if isShutdown {
		return 0, experimentalsys.EIO
	}
	return f.internal.Read(buf)
}

func (f *file) Pread(buf []byte, off int64) (n int, errno experimentalsys.Errno) {
	if isShutdown {
		return 0, experimentalsys.EIO
	}
	return f.internal.Pread(buf, off)
}

func (f *file) Seek(offset int64, whence int) (newOffset int64, errno experimentalsys.Errno) {
	if isShutdown {
		return 0, experimentalsys.EIO
	}
	return f.internal.Seek(offset, whence)
}

func (f *file) Readdir(n int) (dirents []experimentalsys.Dirent, errno experimentalsys.Errno) {
	if isShutdown {
		return nil, experimentalsys.EIO
	}
	return f.internal.Readdir(n)
}

func (f *file) Write(buf []byte) (n int, errno experimentalsys.Errno) {
	if isShutdown {
		return 0, experimentalsys.EIO
	}
	return f.internal.Write(buf)
}

func (f *file) Pwrite(buf []byte, off int64) (n int, errno experimentalsys.Errno) {
	if isShutdown {
		return 0, experimentalsys.EIO
	}
	return f.internal.Pwrite(buf, off)
}

func (f *file) Truncate(size int64) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return f.internal.Truncate(size)
}

func (f *file) Sync() experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return f.internal.Sync()
}

func (f *file) Datasync() experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return f.internal.Datasync()
}

func (f *file) Utimens(atim, mtim int64) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return f.internal.Utimens(atim, mtim)
}

func (f *file) Close() experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return f.internal.Close()
}
