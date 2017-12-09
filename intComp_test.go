package intCompBench

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/jwilder/encoding/simple8b"
	"github.com/smerity/govarint"
)

type postingDetailsStruct struct {
	// Freq/norms data
	freqs []uint64
	norms []float32

	// Location data
	fields   [][]uint16
	pos      [][]uint64
	starts   [][]uint64
	ends     [][]uint64
	arrayPos [][][]uint64
}

func (pds *postingDetailsStruct) length() int {
	total := 0
	total += len(pds.freqs) * 8
	total += len(pds.norms) * 4
	for i := 0; i < len(pds.freqs); i++ {
		total += len(pds.fields[i]) * 2
		total += len(pds.pos[i]) * 8
		total += len(pds.starts[i]) * 8
		total += len(pds.ends[i]) * 8

		for j := 0; j < len(pds.freqs); j++ {
			total += len(pds.arrayPos[i][j]) * 8
		}
	}

	return total
}

var chunkfactor float64
var postings []uint64
var postingDetails *postingDetailsStruct
var numChunks int

func init() {
	chunkfactor = 5.0

	postings = []uint64{
		101, 105, 215, 218, 240,
		260, 280, 290, 320, 325,
		375, 480, 578, 690, 755,
	}

	postingDetails = &postingDetailsStruct{
		freqs:    make([]uint64, len(postings)),
		norms:    make([]float32, len(postings)),
		fields:   make([][]uint16, len(postings)),
		pos:      make([][]uint64, len(postings)),
		starts:   make([][]uint64, len(postings)),
		ends:     make([][]uint64, len(postings)),
		arrayPos: make([][][]uint64, len(postings)),
	}

	for i := 0; i < len(postings); i++ {
		postingDetails.freqs[i] = rand.Uint64() % uint64(1000)
		postingDetails.norms[i] = rand.Float32()

		numFreqs := int(postingDetails.freqs[i])
		postingDetails.fields[i] = make([]uint16, numFreqs)
		postingDetails.pos[i] = make([]uint64, numFreqs)
		postingDetails.starts[i] = make([]uint64, numFreqs)
		postingDetails.ends[i] = make([]uint64, numFreqs)
		postingDetails.arrayPos[i] = make([][]uint64, numFreqs)

		for j := 0; j < int(postingDetails.freqs[i]); j++ {
			postingDetails.fields[i][j] = uint16(rand.Uint32() % uint32(100))
			postingDetails.pos[i][j] = rand.Uint64() % uint64(100)
			postingDetails.starts[i][j] = rand.Uint64() % uint64(100)
			postingDetails.ends[i][j] = rand.Uint64() % uint64(100)

			numArrayPos := int(rand.Uint32() % uint32(10))
			postingDetails.arrayPos[i][j] = make([]uint64, numArrayPos)
			for k := 0; k < numArrayPos; k++ {
				postingDetails.arrayPos[i][j][k] = rand.Uint64() % uint64(100)
			}
		}
	}

	numChunks = int(math.Ceil(float64(len(postings)) / chunkfactor))
}

// smerity/govarint

func TestGovarintBasic(t *testing.T) {
	buf := &bytes.Buffer{}
	x := []uint64{10, 2131, 123123, 1 << 32, 1152921504606846975}
	encoder := govarint.NewU64Base128Encoder(buf)
	for _, entry := range x {
		_, err := encoder.PutU64(entry)
		if err != nil {
			t.Fatalf("Error: Encoding")
		}
	}
	encoder.Close()
	fmt.Printf("Total bytes: %v to %v\n", 8*len(x), buf.Len())

	decoder := govarint.NewU64Base128Decoder(buf)
	for _, entry := range x {
		val, err := decoder.GetU64()
		if err != nil || val != entry {
			t.Fatalf("Error: Decoding")
		}
	}
}

func TestGovarintUsecase(t *testing.T) {
	encoding1 := make([]*bytes.Buffer, numChunks) // Freqs, norms
	encoding2 := make([]*bytes.Buffer, numChunks) // Location
	for i := 0; i < numChunks; i++ {
		encoding1[i] = &bytes.Buffer{}
		encoding2[i] = &bytes.Buffer{}
	}

	var encoder *govarint.Base128Encoder
	var locEncoder *govarint.Base128Encoder

	start := time.Now()
	chunk := -1
	for i := 0; i < len(postings); i++ {
		curChunk := i / int(chunkfactor)

		if curChunk != chunk {
			encoder.Close()
			locEncoder.Close()
			encoder = govarint.NewU64Base128Encoder(encoding1[curChunk])
			locEncoder = govarint.NewU64Base128Encoder(encoding2[curChunk])
			chunk = curChunk
		}

		_, err := encoder.PutU64(postingDetails.freqs[i])
		if err != nil {
			t.Fatalf("[%v] Encoding freq", i)
		}

		norm := postingDetails.norms[i]
		normBits := math.Float32bits(norm)
		_, err = encoder.PutU32(normBits)
		if err != nil {
			t.Fatalf("[%v] Encoding norm", i)
		}

		for j := 0; j < int(postingDetails.freqs[i]); j++ {
			_, err = locEncoder.PutU64(uint64(postingDetails.fields[i][j]))
			if err != nil {
				t.Fatalf("[%v, %v] Encoding field", i, j)
			}

			_, err = locEncoder.PutU64(postingDetails.pos[i][j])
			if err != nil {
				t.Fatalf("[%v, %v] Encoding pos", i, j)
			}

			_, err = locEncoder.PutU64(postingDetails.starts[i][j])
			if err != nil {
				t.Fatalf("[%v, %v] Encoding start", i, j)
			}

			_, err = locEncoder.PutU64(postingDetails.ends[i][j])
			if err != nil {
				t.Fatalf("[%v, %v] Encoding end", i, j)
			}

			numArrayPos := len(postingDetails.arrayPos[i][j])
			_, err = locEncoder.PutU64(uint64(numArrayPos))
			if err != nil {
				t.Fatalf("[%v, %v] Encoding numArrayPos", i, j)
			}

			for k := 0; k < numArrayPos; k++ {
				_, err = locEncoder.PutU64(postingDetails.arrayPos[i][j][k])
				if err != nil {
					t.Fatalf("[%v, %v, %v] Encoding arrayPos", i, j, k)
				}
			}
		}
	}
	timeToEncode := time.Since(start)

	total := 0
	for i := 0; i < numChunks; i++ {
		total += encoding1[i].Len()
		total += encoding2[i].Len()
	}

	fmt.Println("===============================================")
	fmt.Printf("Total bytes: %v to %v\n", postingDetails.length(), total)
	fmt.Println("Time for encoding: ", timeToEncode)
	fmt.Println("===============================================")

	//var decoder *govarint.Base128Decoder
}

// jwilder/encoding

func TestJwilderEncodingBasic(t *testing.T) {
	x := []uint64{10, 2131, 123123, 1 << 32, 1152921504606846975}
	encoder := simple8b.NewEncoder()
	for _, entry := range x {
		encoder.Write(entry)
	}
	buf, err := encoder.Bytes() // buf is a []byte
	if err != nil {
		t.Fatalf("Error: Encoding")
	}
	fmt.Printf("Total bytes: %v to %v\n", 8*len(x), len(buf))

	decoder := simple8b.NewDecoder(buf)
	i := 0
	for decoder.Next() {
		if i >= len(x) {
			t.Fatalf("Error: Decoded more than expected number of entries")
		}

		if decoder.Read() != x[i] {
			t.Fatalf("Error: Decoding")
		}

		i += 1
	}
}

func TestJwilderEncodingUsecase(t *testing.T) {
	encoding1 := make([][]byte, numChunks) // Freqs, norms
	encoding2 := make([][]byte, numChunks) // Location

	var encoder *simple8b.Encoder
	var locEncoder *simple8b.Encoder

	var err error

	start := time.Now()
	chunk := -1
	for i := 0; i < len(postings); i++ {
		curChunk := i / int(chunkfactor)

		if curChunk != chunk {
			if chunk >= 0 {
				encoding1[chunk], err = encoder.Bytes()
				if err != nil {
					t.Fatalf("[%v] Error: Encoding", chunk)
				}
				encoding2[chunk], err = locEncoder.Bytes()
				if err != nil {
					t.Fatalf("[%v] Error: Encoding", chunk)
				}
			}
			encoder = simple8b.NewEncoder()
			locEncoder = simple8b.NewEncoder()
			chunk = curChunk
		}

		err = encoder.Write(postingDetails.freqs[i])
		if err != nil {
			t.Fatalf("[%v] Encoding freq", i)
		}

		norm := postingDetails.norms[i]
		normBits := math.Float32bits(norm)
		err = encoder.Write(uint64(normBits))
		if err != nil {
			t.Fatalf("[%v] Encoding norm", i)
		}

		for j := 0; j < int(postingDetails.freqs[i]); j++ {
			err = locEncoder.Write(uint64(postingDetails.fields[i][j]))
			if err != nil {
				t.Fatalf("[%v, %v] Encoding field", i, j)
			}

			err = locEncoder.Write(postingDetails.pos[i][j])
			if err != nil {
				t.Fatalf("[%v, %v] Encoding pos", i, j)
			}

			err = locEncoder.Write(postingDetails.starts[i][j])
			if err != nil {
				t.Fatalf("[%v, %v] Encoding start", i, j)
			}

			err = locEncoder.Write(postingDetails.ends[i][j])
			if err != nil {
				t.Fatalf("[%v, %v] Encoding end", i, j)
			}

			numArrayPos := len(postingDetails.arrayPos[i][j])
			err = locEncoder.Write(uint64(numArrayPos))
			if err != nil {
				t.Fatalf("[%v, %v] Encoding numArrayPos", i, j)
			}

			for k := 0; k < numArrayPos; k++ {
				err = locEncoder.Write(postingDetails.arrayPos[i][j][k])
				if err != nil {
					t.Fatalf("[%v, %v, %v] Encoding arrayPos", i, j, k)
				}
			}
		}
	}

	if chunk >= 0 {
		encoding1[chunk], err = encoder.Bytes()
		if err != nil {
			t.Fatalf("[%v] Error: Encoding", chunk)
		}
		encoding2[chunk], err = locEncoder.Bytes()
		if err != nil {
			t.Fatalf("[%v] Error: Encoding", chunk)
		}
	}

	timeToEncode := time.Since(start)

	total := 0
	for i := 0; i < numChunks; i++ {
		total += len(encoding1[i])
		total += len(encoding2[i])
	}

	fmt.Println("===============================================")
	fmt.Printf("Total bytes: %v to %v\n", postingDetails.length(), total)
	fmt.Println("Time for encoding: ", timeToEncode)
	fmt.Println("===============================================")

	//var decoder *simple8b.Decoder
}
