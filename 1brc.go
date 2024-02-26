package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"runtime"
	"sort"
	"sync"
	"syscall"
)

type measurement struct {
	min, max, sum, count int64
}

func main() {
	execute(os.Args[1])
}

func execute(fileName string) {
	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	stat, _ := f.Stat()
	size := stat.Size()

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(data)

	cpus := runtime.NumCPU()

	chunkSize := len(data) / cpus
	if chunkSize == 0 {
		chunkSize = len(data)
	}

	chunks := make([]int, 0, cpus)
	offset := 0

	for offset < len(data) {
		offset += chunkSize
		if offset >= len(data) {
			chunks = append(chunks, len(data))
			break
		} else {
			nlPos := bytes.IndexByte(data[offset:], '\n')
			if nlPos == -1 {
				chunks = append(chunks, len(data))
				break
			} else {
				offset += nlPos + 1
				chunks = append(chunks, offset)
			}
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(chunks))

	results := make([]map[string]*measurement, len(chunks))
	start := 0
	for i, chunk := range chunks {
		// go process(data[i*chunkSize : chunks[i]])
		go func(data []byte, i int) {
			results[i] = process(data)
			wg.Done()
		}(data[start:chunk], i)
		start = chunk
	}
	wg.Wait()

	measurements := make(map[string]*measurement)
	for _, r := range results {
		for id, rm := range r {
			m := measurements[id]
			if m == nil {
				measurements[id] = rm
			} else {
				m.min = min(m.min, rm.min)
				m.max = max(m.max, rm.max)
				m.sum += rm.sum
				m.count += rm.count
			}
		}
	}

	ids := make([]string, 0, len(measurements))
	for id := range measurements {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	fmt.Print("{")
	for i, id := range ids {
		if i > 0 {
			fmt.Print(", ")
		}
		m := measurements[id]
		fmt.Printf("%s=%.1f/%.1f/%.1f", id, round(float64(m.min)/10.0), round(float64(m.sum)/10.0/float64(m.count)), round(float64(m.max)/10.0))
	}
	fmt.Println("}")

}

func process(data []byte) map[string]*measurement {
	measurements := make(map[string]*measurement, 0)

	for len(data) > 0 {
		semiPos := 0
		for i, b := range data {
			if b == ';' {
				semiPos = i
				break
			}
		}

		id := string(data[:semiPos])
		data = data[semiPos+1:]

		var temp int64
		// parseNumber
		{
			negative := data[0] == '-'
			if negative {
				data = data[1:]
			}

			_ = data[3]
			if data[1] == '.' {
				// 1.2\n
				temp = int64(data[0])*10 + int64(data[2]) - '0'*(10+1)
				data = data[4:]
				// 12.3\n
			} else {
				_ = data[4]
				temp = int64(data[0])*100 + int64(data[1])*10 + int64(data[3]) - '0'*(100+10+1)
				data = data[5:]
			}

			if negative {
				temp = -temp
			}
		}

		m := measurements[id]
		if m == nil {
			measurements[id] = &measurement{
				min:   temp,
				max:   temp,
				sum:   temp,
				count: 1,
			}
		} else {
			m.min = min(m.min, temp)
			m.max = max(m.max, temp)
			m.sum += temp
			m.count++
		}
	}

	return measurements
}

func round(x float64) float64 {
	return roundJava(x*10.0) / 10.0
}

// roundJava returns the closest integer to the argument, with ties
// rounding to positive infinity, see java's Math.round
func roundJava(x float64) float64 {
	t := math.Trunc(x)
	if x < 0.0 && t-x == 0.5 {
		//return t
	} else if math.Abs(x-t) >= 0.5 {
		t += math.Copysign(1, x)
	}

	if t == 0 { // check -0
		return 0.0
	}
	return t
}
