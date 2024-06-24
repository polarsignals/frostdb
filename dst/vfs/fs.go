package vfs

import (
	"io/fs"

	experimentalsys "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/sys"
)

type dstfs struct {
	// internal is the underlying file system to delegate to. This is
	// purposefully not embedded so that any new methods need to be explicitly
	// added.
	internal experimentalsys.FS
}

var _ experimentalsys.FS = (*dstfs)(nil)

func New(dir string) experimentalsys.FS {
	return &dstfs{internal: sysfs.DirFS(dir)}
}

func (d *dstfs) OpenFile(path string, flag experimentalsys.Oflag, perm fs.FileMode) (experimentalsys.File, experimentalsys.Errno) {
	if isShutdown {
		return nil, experimentalsys.EIO
	}

	f, err := d.internal.OpenFile(path, flag, perm)
	if err != 0 {
		return nil, err
	}
	return newFile(f), 0
}

func (d *dstfs) Lstat(path string) (sys.Stat_t, experimentalsys.Errno) {
	if isShutdown {
		return sys.Stat_t{}, experimentalsys.EIO
	}
	return d.internal.Lstat(path)
}

func (d *dstfs) Stat(path string) (sys.Stat_t, experimentalsys.Errno) {
	if isShutdown {
		return sys.Stat_t{}, experimentalsys.EIO
	}
	return d.internal.Stat(path)
}

func (d *dstfs) Mkdir(path string, perm fs.FileMode) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return d.internal.Mkdir(path, perm)
}

func (d *dstfs) Chmod(path string, perm fs.FileMode) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return d.internal.Chmod(path, perm)
}

func (d *dstfs) Rename(from, to string) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return d.internal.Rename(from, to)
}

func (d *dstfs) Rmdir(path string) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return d.internal.Rmdir(path)
}

func (d *dstfs) Unlink(path string) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return d.internal.Unlink(path)
}

func (d *dstfs) Link(oldPath, newPath string) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return d.internal.Link(oldPath, newPath)
}

func (d *dstfs) Symlink(oldPath, linkName string) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return d.internal.Symlink(oldPath, linkName)
}

func (d *dstfs) Readlink(path string) (string, experimentalsys.Errno) {
	if isShutdown {
		return "", experimentalsys.EIO
	}
	return d.internal.Readlink(path)
}

func (d *dstfs) Utimens(path string, atim, mtim int64) experimentalsys.Errno {
	if isShutdown {
		return experimentalsys.EIO
	}
	return d.internal.Utimens(path, atim, mtim)
}
