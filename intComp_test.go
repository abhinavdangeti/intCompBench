package intCompBench

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/abhinavdangeti/reductor"
	"github.com/jwilder/encoding/simple8b"
	"github.com/smerity/govarint"
)

type postingDetailsStruct struct {
	// Freq/norms data
	freqs []uint64
	norms []float32

	// Location data
	fields   [][]uint64 // Actually a uint16, but for testing purposes.
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
		total += len(pds.fields[i]) * 2 // Since this is originally a uint16
		total += len(pds.pos[i]) * 8
		total += len(pds.starts[i]) * 8
		total += len(pds.ends[i]) * 8

		for j := 0; j < len(pds.arrayPos[i]); j++ {
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
		1400, 1592, 1946, 2000, 2239,
		34, 556, 600, 1234, 1270,
		4780, 5290, 6992, 7000, 8262,
		29590, 39200, 59109, 82693, 100351,
		2500, 2501, 2503, 3991, 4728,
		13892, 15001, 15002, 18269, 28651,
		9618, 9762, 9872, 10021, 10245,
	}

	postingDetails = &postingDetailsStruct{
		freqs:    make([]uint64, len(postings)),
		norms:    make([]float32, len(postings)),
		fields:   make([][]uint64, len(postings)),
		pos:      make([][]uint64, len(postings)),
		starts:   make([][]uint64, len(postings)),
		ends:     make([][]uint64, len(postings)),
		arrayPos: make([][][]uint64, len(postings)),
	}

	for i := 0; i < len(postings); i++ {
		postingDetails.freqs[i] = rand.Uint64() % uint64(1000)
		postingDetails.norms[i] = rand.Float32()

		numFreqs := int(postingDetails.freqs[i])
		postingDetails.fields[i] = make([]uint64, numFreqs)
		postingDetails.pos[i] = make([]uint64, numFreqs)
		postingDetails.starts[i] = make([]uint64, numFreqs)
		postingDetails.ends[i] = make([]uint64, numFreqs)
		postingDetails.arrayPos[i] = make([][]uint64, numFreqs)

		for j := 0; j < numFreqs; j++ {
			postingDetails.fields[i][j] = rand.Uint64() % uint64(100)
			postingDetails.pos[i][j] = rand.Uint64() % uint64(1000)
			postingDetails.starts[i][j] = rand.Uint64() % uint64(1000)
			postingDetails.ends[i][j] = rand.Uint64() % uint64(1000)

			numArrayPos := int(rand.Uint32() % uint32(25))
			postingDetails.arrayPos[i][j] = make([]uint64, numArrayPos)
			for k := 0; k < numArrayPos; k++ {
				postingDetails.arrayPos[i][j][k] = rand.Uint64() % uint64(1000)
			}
		}
	}

	numChunks = int(math.Ceil(float64(len(postings)) / chunkfactor))
}

func emitEncodingResults(encodedFootPrint int, timeToEncode time.Duration) {
	fmt.Println("===============================================")
	fmt.Println("Actual footprint: ", postingDetails.length())
	fmt.Println("Encoded footprint: ", encodedFootPrint)
	fmt.Printf("Reduction in footprint: %.4v%%\n",
		float32((postingDetails.length()-encodedFootPrint)*100)/float32(postingDetails.length()))
	fmt.Println("Time for encoding: ", timeToEncode)
	fmt.Println("===============================================")
}

// smerity/govarint

func TestGovarintBasic(t *testing.T) {
	buf := &bytes.Buffer{}
	x := []uint64{280, 105, 215, 690, 240, 578, 101, 320, 755, 325, 375, 480, 260, 218, 290}
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
			_, err = locEncoder.PutU64(postingDetails.fields[i][j])
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

	emitEncodingResults(total, timeToEncode)

	//var decoder *govarint.Base128Decoder
}

// jwilder/encoding

func TestJwilderEncodingBasic(t *testing.T) {
	x := []uint64{280, 105, 215, 690, 240, 578, 101, 320, 755, 325, 375, 480, 260, 218, 290}
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
			err = locEncoder.Write(postingDetails.fields[i][j])
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

	emitEncodingResults(total, timeToEncode)

	//var decoder *simple8b.Decoder
}

// Hybrid: govarint + reductor

type hybridGovarintReductor struct {
	freqs *bytes.Buffer
	norms *bytes.Buffer

	fields []*reductor.DeltaCompPostings
	pos    []*reductor.DeltaCompPostings
	starts []*reductor.DeltaCompPostings
	ends   []*reductor.DeltaCompPostings

	arrayPos [][]*reductor.DeltaCompPostings
}

func newHybridGovarintReductor() *hybridGovarintReductor {
	return &hybridGovarintReductor{
		freqs: &bytes.Buffer{},
		norms: &bytes.Buffer{},
	}
}

func (hgr *hybridGovarintReductor) size() int {
	total := hgr.freqs.Len() + hgr.norms.Len()
	for i := 0; i < len(hgr.fields); i++ {
		total += hgr.fields[i].SizeInBytes()
		total += hgr.pos[i].SizeInBytes()
		total += hgr.starts[i].SizeInBytes()
		total += hgr.ends[i].SizeInBytes()

		for j := 0; j < len(hgr.arrayPos[i]); j++ {
			total += hgr.arrayPos[i][j].SizeInBytes()
		}
	}

	return total
}

func TestGovarintReductorUsecase(t *testing.T) {
	encoding := make([]*hybridGovarintReductor, numChunks)
	for i := 0; i < numChunks; i++ {
		encoding[i] = newHybridGovarintReductor()
	}

	var freqsEncoder *govarint.Base128Encoder
	var normsEncoder *govarint.Base128Encoder

	start := time.Now()
	chunk := -1
	for i := 0; i < len(postings); i++ {
		curChunk := i / int(chunkfactor)

		if curChunk != chunk {
			freqsEncoder.Close()
			normsEncoder.Close()
			freqsEncoder = govarint.NewU64Base128Encoder(encoding[curChunk].freqs)
			normsEncoder = govarint.NewU64Base128Encoder(encoding[curChunk].norms)
			chunk = curChunk
		}

		_, err := freqsEncoder.PutU64(postingDetails.freqs[i])
		if err != nil {
			t.Fatalf("[%v] Encoding freq", i)
		}

		norm := postingDetails.norms[i]
		normBits := math.Float32bits(norm)
		_, err = normsEncoder.PutU32(normBits)
		if err != nil {
			t.Fatalf("[%v] Encoding norm", i)
		}

		cursor := len(encoding[curChunk].fields)

		encoding[curChunk].fields = append(encoding[curChunk].fields, reductor.NewDeltaCompPostings())
		encoding[curChunk].fields[cursor].Encode(postingDetails.fields[i])

		encoding[curChunk].pos = append(encoding[curChunk].pos, reductor.NewDeltaCompPostings())
		encoding[curChunk].pos[cursor].Encode(postingDetails.pos[i])

		encoding[curChunk].starts = append(encoding[curChunk].starts, reductor.NewDeltaCompPostings())
		encoding[curChunk].starts[cursor].Encode(postingDetails.starts[i])

		encoding[curChunk].ends = append(encoding[curChunk].ends, reductor.NewDeltaCompPostings())
		encoding[curChunk].ends[cursor].Encode(postingDetails.ends[i])

		encoding[curChunk].arrayPos = append(encoding[curChunk].arrayPos, []*reductor.DeltaCompPostings{})
		encoding[curChunk].arrayPos[cursor] = make([]*reductor.DeltaCompPostings, postingDetails.freqs[i])

		for j := 0; j < int(postingDetails.freqs[i]); j++ {

			encoding[curChunk].arrayPos[cursor][j] = reductor.NewDeltaCompPostings()
			encoding[curChunk].arrayPos[cursor][j].Encode(postingDetails.arrayPos[i][j])
		}
	}
	timeToEncode := time.Since(start)

	total := 0
	for i := 0; i < len(encoding); i++ {
		total += encoding[i].size()
	}

	emitEncodingResults(total, timeToEncode)
}
