package arcticdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync/atomic"
)

type LogFile struct {
	*os.File
	size uint64
}

func OpenLogFile(name string) (*LogFile, error) {
	file, err := os.OpenFile(name, os.O_RDONLY, 0666)
	if err != nil {
		return nil, err
	}

	size := uint64(0)
	fstat, err := os.Stat(name)
	if err == nil {
		size = uint64(fstat.Size())
	}

	if errors.Is(err, os.ErrNotExist) {
		err = nil
	}

	return &LogFile{
		File: file,
		size: size,
	}, err
}

func CreateLogFile(name string) (*LogFile, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, err
	}

	return &LogFile{
		File: file,
		size: 0,
	}, err
}

func computeChecksum(data []byte) uint32 {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	size := uint32(len(data))
	binary.Write(crc, binary.BigEndian, &size)
	crc.Write(data)
	return crc.Sum32()
}

func (lf *LogFile) encodeRecord(data []byte) ([]byte, error) {
	hdr := &struct {
		Checksum uint32
		Size     uint32
	}{
		Checksum: computeChecksum(data),
		Size:     uint32(len(data)),
	}

	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, hdr)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(data)
	return buf.Bytes(), err
}

func (lf *LogFile) WriteRecord(data []byte) error {
	encoded, err := lf.encodeRecord(data)
	if err != nil {
		return err
	}
	n, err := lf.File.Write(encoded)
	if err != nil {
		return err
	}

	atomic.AddUint64(&lf.size, uint64(n))
	return err
}

func (lf *LogFile) ReadRecord(offset uint64) (int, []byte, error) {
	hdr := &struct {
		Checksum uint32
		Size     uint32
	}{}

	hdrBuf := make([]byte, binary.Size(hdr))
	n, err := lf.ReadAt(hdrBuf, int64(offset))
	if err != nil {
		return n, nil, err
	}

	if err := binary.Read(bytes.NewBuffer(hdrBuf), binary.BigEndian, hdr); err != nil {
		return n, nil, err
	}

	buf := make([]byte, hdr.Size)
	m, err := lf.ReadAt(buf, int64(offset)+int64(n))
	return n + m, buf, err
}

func (lf *LogFile) NewIterator() *LogFileIterator {
	return &LogFileIterator{
		lf:             lf,
		limit:          lf.size,
		currReadOffset: 0,
	}
}

func (lf *LogFile) Size() int {
	return int(atomic.LoadUint64(&lf.size))
}

type LogFileIterator struct {
	currReadOffset uint64
	limit          uint64
	lf             *LogFile
}

func (it *LogFileIterator) HasNext() bool {
	return it.currReadOffset < it.limit
}

func (it *LogFileIterator) NextRecord() ([]byte, error) {
	n, data, err := it.lf.ReadRecord(uint64(it.currReadOffset))
	it.currReadOffset += uint64(n)
	return data, err
}
