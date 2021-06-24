package main

import (
	"hash/crc32"
	"os"
	"sync"
	"time"
)

const (
	T_SIZE = 8
	C_SIZE = 4

	CRC_SIZE       = T_SIZE + C_SIZE
	TOMBSTONE_SIZE = CRC_SIZE + 1
	KEY_SIZE       = TOMBSTONE_SIZE + T_SIZE
	VALUE_SIZE     = KEY_SIZE + T_SIZE

	WAL_EXT     = "wal"
	END_EXT     = "_END"
	FORMAT_NAME = "00000000000000000000%d"
)

type WAL struct {
	mu          sync.Mutex
	path        string
	segments    []*Segment
	tail        *os.File
	maxSize     int64
	d           time.Duration
	lowMark     int
	lastIndex   int64
	lastSegment *Segment
}

type Segment struct {
	path   string
	index  int64
	size   int64
	data   []byte
	synced bool
}

type Entry struct {
	Crc       uint32
	Timestamp uint64
	Deleted   bool
	Key       string
	Value     []byte
}

func (s *Segment) Size() int64 {
	return s.size
}

func (s *Segment) Path() string {
	return s.path
}

func (s *Segment) Data() []byte {
	return s.data
}

func (s *Segment) SetData(data []byte) {
	s.data = append(s.data, data...)
}

func (s *Segment) Append(data []byte, size int64) {
	s.data = append(s.data, data...)
	s.size = s.size + size
}

func (s *Segment) SetSynced(synced bool) {
	s.synced = synced
}

func (s *Segment) IsSynced() bool {
	return s.synced
}

func (s *Segment) Index() int64 {
	return s.index
}

func CRC32(str string) uint32 {
	return crc32.ChecksumIEEE([]byte(str))
}

func (wal *WAL) Close() {
	wal.tail.Close()
}

func (wal *WAL) Segments() []*Segment {
	return wal.segments
}

func (wal *WAL) AppendSegment(segment *Segment) {
	wal.segments = append(wal.segments, segment)
}

func (wal *WAL) removeIndex(index int) {
	wal.segments = append(wal.segments[:index], wal.segments[index+1:]...)
}
