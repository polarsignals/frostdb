package arcticdb

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"sync/atomic"
)

type BlockFile struct {
	*os.File
	size uint64
}

func OpenBlockFile(name string) (*BlockFile, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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

	return &BlockFile{
		File: file,
		size: size,
	}, err
}

func computeChecksum(data []byte) uint32 {
	crc := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	size := uint32(len(data))
	binary.Write(crc, binary.BigEndian, &size)
	crc.Write(data)
	return crc.Sum32()
}

const headerSize = 16

type blockHeader struct {
	Checksum  uint32
	Timestamp uint64
	Size      uint32
}

func (lf *BlockFile) encodeBlock(timestamp uint64, data []byte) ([]byte, error) {
	hdr := &blockHeader{
		Checksum:  computeChecksum(data),
		Size:      uint32(len(data)),
		Timestamp: timestamp,
	}

	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, hdr)
	if err != nil {
		return nil, err
	}

	_, err = buf.Write(data)
	return buf.Bytes(), err
}

func (lf *BlockFile) WriteBlock(timestamp uint64, data []byte) error {
	encoded, err := lf.encodeBlock(timestamp, data)
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

func (lf *BlockFile) ReadBlock(offset uint64) (uint64, []byte, error) {
	hdr := &blockHeader{}

	hdrBuf := make([]byte, binary.Size(hdr))
	_, err := lf.ReadAt(hdrBuf, int64(offset))
	if err != nil {
		return 0, nil, err
	}

	if err := binary.Read(bytes.NewBuffer(hdrBuf), binary.BigEndian, hdr); err != nil {
		return 0, nil, err
	}

	buf := make([]byte, hdr.Size)
	_, err = lf.ReadAt(buf, int64(offset)+int64(headerSize))
	return hdr.Timestamp, buf, err
}

func (lf *BlockFile) NewIterator() *BlockFileIterator {
	return &BlockFileIterator{
		lf:             lf,
		limit:          lf.size,
		currReadOffset: 0,
	}
}

func (lf *BlockFile) Size() int {
	return int(atomic.LoadUint64(&lf.size))
}

type BlockFileIterator struct {
	currReadOffset uint64
	limit          uint64
	lf             *BlockFile
}

func (it *BlockFileIterator) HasNext() bool {
	return it.currReadOffset < it.limit
}

func (it *BlockFileIterator) NextBlock() (uint64, []byte, error) {
	timestamp, data, err := it.lf.ReadBlock(uint64(it.currReadOffset))
	it.currReadOffset += uint64(len(data) + headerSize)
	return timestamp, data, err
}
