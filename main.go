package main

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

/*
+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
|    CRC (4B)   | Timestamp (16B) | Tombstone(1B) | Key Size (8B) | Value Size (8B) | Key | Value |
+---------------+-----------------+---------------+---------------+-----------------+-...-+--...--+
CRC = 32bit hash computed over the payload using CRC
Key Size = Length of the Key data
Tombstone = If this record was deleted and has a value
Value Size = Length of the Value data
Key = Key data
Value = Value data
Timestamp = Timestamp of the operation in seconds
*/

func (wal *WAL) Process(key string, value []byte, deleted bool) []byte {
	data := []byte{}

	crcb := make([]byte, C_SIZE)
	binary.LittleEndian.PutUint32(crcb, CRC32(string(value)))
	data = append(data, crcb...)

	sec := time.Now().Unix()
	secb := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(secb, uint64(sec))
	data = append(data, secb...)

	//0 alive 1 deleted
	if deleted {
		data = append(data, 1)
	} else {
		data = append(data, 0)
	}

	keyb := []byte(key)
	keybs := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(keybs, uint64(len(keyb)))

	valuebs := make([]byte, T_SIZE)
	binary.LittleEndian.PutUint64(valuebs, uint64(len(value)))

	data = append(data, keybs...)
	data = append(data, valuebs...)

	data = append(data, key...)
	data = append(data, value...)

	return data
}

func (wal *WAL) convert(data []byte) []Entry {
	rez := []Entry{}
	if len(data) == 0 {
		return rez
	}

	i := uint64(0)
	for i < uint64(len(data)) {
		crc := binary.LittleEndian.Uint32(data[i : i+C_SIZE])
		timestamp := binary.LittleEndian.Uint64(data[i+C_SIZE : i+CRC_SIZE])
		tombstone := data[i+CRC_SIZE]
		key_size := binary.LittleEndian.Uint64(data[i+TOMBSTONE_SIZE : i+KEY_SIZE])
		value_size := binary.LittleEndian.Uint64(data[i+KEY_SIZE : i+VALUE_SIZE])
		key_data := string(data[i+VALUE_SIZE : i+VALUE_SIZE+key_size])
		val := data[i+VALUE_SIZE+key_size : i+VALUE_SIZE+key_size+value_size]

		b := false
		if tombstone == 1 {
			b = true
		}

		e := Entry{
			crc,
			timestamp,
			b,
			key_data,
			val,
		}
		rez = append(rez, e)

		// valculate new index
		i = i + VALUE_SIZE + key_size + value_size
	}
	return rez
}

func (wal *WAL) Read(index int64) ([]byte, error) {
	// Test the last segment first
	if index >= wal.lastIndex {
		segment, err := wal.getLastSegment()
		if err != nil {
			return nil, err
		}
		return segment.getSegmentData()
	}

	//search in all segments
	segment, err := wal.findSegment(index)
	if err != nil {
		return nil, err
	}
	return segment.getSegmentData()
}

func (wal *WAL) ReadConverted(index int64) ([]Entry, error) {
	// Test the last segment first
	if index >= wal.lastIndex {
		segment, err := wal.getLastSegment()
		if err != nil {
			return nil, err
		}
		bytes, err := segment.getSegmentData()
		if err != nil {
			return nil, err
		}
		return wal.convert(bytes), nil
	}

	//search in all segments
	segment, err := wal.findSegment(index)
	if err != nil {
		return nil, err
	}
	bytes, err := segment.getSegmentData()
	if err != nil {
		return nil, err
	}
	return wal.convert(bytes), nil
}

func (wal *WAL) Set(key string, value []byte, deleted bool) error {
	data := wal.Process(key, value, deleted)
	dataSize := int64(len(data))
	tail, err := wal.getLastSegment()
	if err != nil {
		if tail.Size()+dataSize <= wal.maxSize {
			tail.Append(data, dataSize)
			tail.SetSynced(false)
		} else {
			//Flush previos segment data to disk
			if !tail.IsSynced() {
				wal.Flush()
				tail.SetSynced(true)
			}

			//Create new segment and append data
			newTail, err := wal.newSegment()
			if err != nil {
				return err
			}

			newTail.Append(data, dataSize)
		}
	}
	return err
}

func (wal *WAL) Flush() error {
	tail, err := wal.getLastSegment()
	if err != nil {
		n, err := wal.tail.Write(tail.Data())
		if err != nil {
			return err
		}

		if int64(n) != tail.Size() {
			return errors.New("Error writing data to segment file")
		}

		fmt.Println("Flush!!")
	}
	return err
}

func (wal *WAL) Open() error {
	wal.mu.Lock()
	defer wal.mu.Unlock()
	err := filepath.Walk(wal.path, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() || filepath.Ext(path) != ".wal" {
			return nil
		}

		name := fileNameWithoutExtension(info.Name())
		var i int64
		if strings.HasSuffix(name, END_EXT) {
			lastIndex := fileNameWithoutSuffix(name, END_EXT)
			i, err = convertIndex(lastIndex)
			if err != nil {
				return err
			}
			wal.lastIndex = i
		} else {
			i, err = convertIndex(name)
			if err != nil {
				return err
			}
		}

		fi, err := os.Stat(path)
		if err != nil {
			return err
		}

		segment := &Segment{
			path:   path,
			index:  i,
			size:   fi.Size(),
			synced: true,
		}
		wal.AppendSegment(segment)
		return nil
	})
	if err != nil {
		return err
	}
	return wal.setupLastSegment()
}

func (tail *Segment) loadSegmentData() error {
	data, err := ioutil.ReadFile(tail.path)
	if err != nil {
		return err
	}
	tail.SetData(data)
	return nil
}

func (tail *Segment) getSegmentData() ([]byte, error) {
	data, err := ioutil.ReadFile(tail.path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (wal *WAL) getLastSegment() (*Segment, error) {
	// Try to found last segment, if exists, otherwise create new segment and return as last
	i := sort.Search(len(wal.segments), func(i int) bool { return wal.lastIndex <= wal.segments[i].index })
	if i < len(wal.segments) && wal.segments[i].index == wal.lastIndex {
		return wal.segments[i], nil
	} else {
		return wal.newSegment()
	}
}

func (wal *WAL) findSegment(index int64) (*Segment, error) {
	// Try to found last segment, if exists, otherwise create new segment and return as last
	i := sort.Search(len(wal.segments), func(i int) bool { return index <= wal.segments[i].index })
	if i < len(wal.segments) && wal.segments[i].index == index {
		return wal.segments[i], nil
	} else {
		return nil, nil
	}
}

func (wal *WAL) setupLastSegment() error {
	lastSegment, err := wal.getLastSegment()
	if err == nil {
		//Open file
		wal.tail, err = os.OpenFile(lastSegment.Path(), os.O_WRONLY, 0666) // open as WRITE ONLY
		if err != nil {
			return err
		}

		// Set that data will be appended to file
		if _, err = wal.tail.Seek(0, 2); err != nil { // append only to end of file
			return err
		}

		//Fill data to memory from last segment
		lastSegment.loadSegmentData()
	}
	return err
}

func (wal *WAL) newSegment() (*Segment, error) {
	//Close the previous tail file
	prevTail := wal.tail.Name()
	wal.tail.Close()

	//Rename previous last segment and remove _END mark and append to new one
	regularPath := strings.Replace(prevTail, END_EXT, "", -1)
	err := os.Rename(prevTail, regularPath)
	if err != nil {
		return nil, err
	}

	//Create new segment file and assign to tail
	index := int64(wal.lastIndex + 1)
	temp := fmt.Sprintf(FORMAT_NAME, index)
	temp = temp[len(temp)-20:]
	temp = strings.Join([]string{wal.path, temp}, string(os.PathSeparator))
	temp = strings.Join([]string{temp, END_EXT}, "")
	temp = strings.Join([]string{temp, WAL_EXT}, ".")

	wal.tail, err = os.Create(temp)
	if err != nil {
		return nil, err
	}

	segment := &Segment{
		index:  index,
		path:   temp,
		synced: false,
	}

	wal.lastIndex = index
	wal.AppendSegment(segment)
	return segment, nil
}

func (wal *WAL) cleanLog() {
	wal.mu.Lock()
	defer wal.mu.Unlock()

	for i := len(wal.segments) - 1; i >= wal.lowMark; i-- {
		err := os.Remove(wal.segments[i].Path())
		if err != nil {
			fmt.Println(err)
			return
		}
		wal.removeIndex(i)
	}
}

func (wal *WAL) clean(ctx context.Context) {
	go func() {
		for {
			select {
			case <-time.Tick(wal.d):
				wal.cleanLog()
			case <-ctx.Done():
				return
			}
		}
	}()
}

func NewWAL(path string, maxSize int64, duration time.Duration, lowMark int) *WAL {
	return &WAL{
		path:      path,
		segments:  []*Segment{},
		maxSize:   maxSize,
		d:         duration,
		lowMark:   lowMark,
		lastIndex: -1,
	}
}

func main() {
	wal := NewWAL("/Users/milossimic/Desktop/wal", 100, time.Second, 2) //20971520) //20MB segment size
	err := wal.Open()
	if err != nil {
		fmt.Println(err)
		return
	}
	// for _, s := range wal.Segments() {
	// 	fmt.Println(s)
	// }

	// wal.Set("key", []byte{1, 6}, false)
	// wal.Set("key1", []byte{1, 6}, false)
	// wal.Set("key2", []byte{1, 6}, false)
	// fmt.Println(wal.TailSegment().Data())

	s, err := wal.ReadConverted(2)
	if err != nil {
		fmt.Println(err)
	}
	for _, v := range s {
		fmt.Println(v)
	}

	// wal.Flush()
	wal.Close()
}
