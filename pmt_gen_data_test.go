package pebble

import (
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"testing"
)

func Test_gen_data(t *testing.T) {
	const times = 128
	deviation := uint64(1 << 32)
	dir := datasetPath
	for i := 0; i < times; i++ {
		d := NewData()
		mean := 1024*math.MaxUint32 + uint64(i)*(math.MaxUint32)
		d = d.AddNormal(mean, deviation, 1<<20, MinKey+1, MaxKey-1)
		d = d.AddUniform(MinKey+1, MaxKey-1, 1024)
		d = d.AddMinMax()
		sort.Slice(d.Keys, func(i, j int) bool { return d.Keys[i] < d.Keys[j] })
		path := filepath.Join(dir, fmt.Sprintf("normal_plus_round_%03d.bin", i))
		SaveDataFile(path, d)
	}
	for i := 0; i < times; i++ {
		d := NewData()
		d = d.AddUniform(MinKey, MaxKey, 1<<20)
		d = d.AddMinMax()
		sort.Slice(d.Keys, func(i, j int) bool { return d.Keys[i] < d.Keys[j] })
		path := filepath.Join(dir, fmt.Sprintf("uniform_round_%03d.bin", i))
		SaveDataFile(path, d)
	}
}
