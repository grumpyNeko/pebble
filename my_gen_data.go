package pebble

import (
	"bufio"
	"encoding/binary"
	"math"
	"math/rand"
	"os"
	"path/filepath"
)

var genKeysR = rand.New(rand.NewSource(1))
var MinKey uint64 = 0
var MaxKey uint64 = math.MaxUint64 - 1

type Data struct {
	Keys []uint64
	M    map[uint64]struct{}
	Size int
}

func NewData() *Data {
	size := 1 << 20
	return &Data{
		Keys: make([]uint64, 0, size),
		M:    make(map[uint64]struct{}, size),
		Size: size,
	}
}

func (d *Data) AddNormal(mean uint64, deviation uint64, size int, low uint64, high uint64) *Data {
	if high < low {
		panic("")
	}
	rangeSize := high - low + 1
	if uint64(size) > rangeSize {
		panic("")
	}

	d.Keys = d.Keys[:d.Size-size]
	for len(d.Keys) < size {
		// rand * deviation + mean
		k := uint64(genKeysR.NormFloat64()*float64(deviation) + float64(mean))
		if _, ok := d.M[k]; ok {
			continue
		}
		if k == MinKey || k == MaxKey {
			continue
		}
		if k < low || k > high {
			continue
		}
		d.Keys = append(d.Keys, k)
		d.M[k] = struct{}{}
	}
	return d
}

func (d *Data) Shuffle() *Data {
	rand.Shuffle(len(d.Keys), func(i, j int) {
		d.Keys[i], d.Keys[j] = d.Keys[j], d.Keys[i]
	})
	return d
}

func (d *Data) AddUniform(low, high uint64, size int) *Data {
	if high < low {
		panic("")
	}
	rangeSize := high - low + 1
	if uint64(size) > rangeSize {
		panic("")
	}

	d.Keys = d.Keys[:d.Size-size]
	for len(d.Keys) < cap(d.Keys) { // TODO: cap not good
		k := low + genKeysR.Uint64()%rangeSize
		if _, ok := d.M[k]; ok {
			continue
		}
		if k == 0 || k == math.MaxUint64-1 {
			continue
		}
		d.Keys = append(d.Keys, k)
		d.M[k] = struct{}{}
	}
	return d
}

func (d *Data) AddMinMax() *Data {
	d.Keys = d.Keys[:len(d.Keys)-2]
	d.Keys = append(d.Keys, MinKey)
	d.M[MinKey] = struct{}{}
	d.Keys = append(d.Keys, MaxKey)
	d.M[MaxKey] = struct{}{}
	return d
}

func (d *Data) Dump(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)

	for _, k := range d.Keys {
		// Write each uint64 key to the buffer
		if err := binary.Write(writer, binary.LittleEndian, k); err != nil {
			panic(err)
		}
	}
	err = writer.Flush()
	if err != nil {
		panic(err)
	}
}

func Load(filename string) *Data {
	stat, err := os.Stat(filename)
	if err != nil {
		panic(err)
	}
	if stat.Size()%8 != 0 {
		panic("invalid file size")
	}
	count := stat.Size() / 8

	fileBytes, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}

	data := &Data{
		Keys: make([]uint64, 0, count),
		M:    make(map[uint64]struct{}, count),
	}
	for i := int64(0); i < count; i++ {
		base := i * 8
		k := binary.LittleEndian.Uint64(fileBytes[base : base+8])
		data.Keys = append(data.Keys, k)
		data.M[k] = struct{}{}
	}
	return data
}

const (
	pmtDatasetFileMode  = 0o755
	pmtDatasetWriteMode = 0o644
	pmtDataKeysPerFile  = 1 << 20
)

// one-file-per-Data(exactly 1M uint64)
func SaveDataFile(filename string, d *Data) {
	if d == nil {
		panic("nil data")
	}
	if len(d.Keys) != pmtDataKeysPerFile {
		panic("invalid data size")
	}
	dir := filepath.Dir(filename)
	if dir != "." && dir != "" {
		if err := os.MkdirAll(dir, pmtDatasetFileMode); err != nil {
			panic(err)
		}
	}

	file, err := os.OpenFile(filename, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, pmtDatasetWriteMode)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, k := range d.Keys {
		if err := binary.Write(writer, binary.LittleEndian, k); err != nil {
			panic(err)
		}
	}
	if err := writer.Flush(); err != nil {
		panic(err)
	}
}

// LoadDataFile reads a single Data (1M uint64 keys) from disk.
func LoadDataFile(filename string) *Data {
	data, err := os.ReadFile(filename)
	if err != nil {
		panic(err)
	}
	if len(data)%8 != 0 {
		panic("invalid dataset: size not aligned")
	}
	count := len(data) / 8
	if count != pmtDataKeysPerFile {
		panic("invalid dataset: unexpected key count")
	}
	keys := make([]uint64, count)
	for i := 0; i < count; i++ {
		base := i * 8
		keys[i] = binary.LittleEndian.Uint64(data[base : base+8])
	}
	ret := &Data{
		Keys: keys,
		M:    make(map[uint64]struct{}, count),
		Size: count,
	}
	for _, k := range keys {
		ret.M[k] = struct{}{}
	}
	return ret
}
