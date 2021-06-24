package main

import (
	"errors"
	"github.com/MilosSimic/fmmap"
	"github.com/MilosSimic/lru"
	"hash/crc32"
	"os"
	"strconv"
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
	mu        sync.Mutex
	path      string
	segments  []*Segment
	tail      *fmmap.FMMAP
	d         time.Duration
	lowMark   int
	lastIndex int64
	cache     *lru.LRU
}

type Segment struct {
	path  string
	index int64
	size  int64
	data  []byte
}

type Entry struct {
	Crc       uint32
	Timestamp uint64
	Deleted   bool
	Key       string
	Value     []byte
}

type T struct {
	Key     string
	Value   []byte
	Deleted bool
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

func NewWAL(path string, duration time.Duration, lowMark int, cap int) (*WAL, error) {
	cache, err := lru.NewLRU(cap, nil)
	if err != nil {
		return nil, err
	}

	return &WAL{
		path:      path,
		segments:  []*Segment{},
		d:         duration,
		lowMark:   lowMark,
		lastIndex: 0,
		cache:     cache,
	}, nil
}

func (wal *WAL) TailPath() (string, error) {
	if wal.tail == nil {
		return "", errors.New("no tail file")
	}
	return wal.tail.GetFile().Name(), nil
}

func open(path string) (*fmmap.FMMAP, error) {
	return fmmap.NewFile(path, os.O_RDWR|os.O_CREATE)
}

func (wal *WAL) Update(data []byte, s *Segment) error {
	err := wal.tail.Update(data)
	if err != nil {
		return err
	}
	s.Append(data, int64(len(data)))
	return nil
}

func (wal *WAL) findInCache(index int64) ([]byte, error) {
	key := strconv.Itoa(int(index))
	v, ok := wal.cache.Get(key)
	if !ok {
		return nil, errors.New("Cache miss!")
	}
	val := v.(*lru.Elem).Value
	s, ok := val.([]byte)
	if !ok {
		return nil, errors.New("Conversion error")
	}
	return s, nil
}

func (wal *WAL) cacheit(index int64, value []byte) error {
	key := strconv.Itoa(int(index))
	_, ok := wal.cache.Put(key, value)
	if !ok {
		return errors.New("Cache error")
	}
	return nil
}
