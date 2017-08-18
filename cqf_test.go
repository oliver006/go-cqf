package cqf

import (
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"
)

func getCQF() *CQF {
	cqf, _ := NewCQF()
	return cqf
}

func TestNew(t *testing.T) {
	_, err := NewCQF()
	if err != nil {
		t.Errorf("NewCQF() err: %s", err)
	}
}

func TestNoInsert(t *testing.T) {
	c := getCQF()
	if count := c.CountHash(uint32(0)); count != 0 {
		t.Errorf("shouldn't have found uint(0)")
	}
}

func TestZero(t *testing.T) {
	c := getCQF()
	c.InsertHash(uint32(1), 1)
	if count := c.CountHash(uint32(0)); count != 0 {
		t.Errorf("shouldn't have found uint(0)")
	}
}

func TestSimpleInsert(t *testing.T) {
	c := getCQF()

	key1 := uint32(8)
	key2 := uint32(789010101)
	c.InsertHash(key1, 1)
	c.InsertHash(key2, 1)

	if count := c.CountHash(key1); count != 1 {
		t.Errorf("nope1, count is: %d", count)
	}
	if count := c.CountHash(key1); count != 1 {
		t.Errorf("nope1, count is: %d", count)
	}
}

func TestMultipleKeys(t *testing.T) {

	c := getCQF()

	keys := map[uint32]bool{
		1:     true,
		1024:  true,
		16383: true,
		16384: true,
	}

	var maxVal uint32
	for k, _ := range keys {
		c.InsertHash(k, 1)
		if k > maxVal {
			maxVal = k + 1
		}
	}

	var lookupKey uint32
	for lookupKey < maxVal {
		if _, ok := keys[lookupKey]; !ok {
			if count := c.CountHash(lookupKey); count != 0 {
				t.Errorf("shouldn't have found something for key: %d", lookupKey)
			}
		}
		lookupKey++
	}
}

func TestAlottOfRandomHashes(t *testing.T) {
	c := getCQF()

	count := uint64(1) << 16
	for count > 0 {
		hash := uint32(rand.Int31())
		c.InsertHash(hash, 1)
		if c.CountHash(hash) == 0 {
			t.Errorf("Didn't find %d", hash)
			return
		}
		count--
	}
}

func TestAlottOfTheSame(t *testing.T) {
	c := getCQF()

	hash := uint32(400000)
	count := rand.Int31n(1345700) + 100000
	amountWant := uint64(count)
	for count > 0 {
		c.InsertHash(hash, 1)
		count--
	}

	if got := c.CountHash(hash); got != amountWant {
		t.Errorf("got: %d, expected: %d", got, amountWant)
	}

}

func TestMultiInsert(t *testing.T) {
	c := getCQF()

	hash := uint32(8)
	lastAmount := uint64(0)
	c.InsertHash(hash, 1)

	count := 4
	for count > 0 {
		c.InsertHash(hash, 1)

		if count := c.CountHash(hash); count == lastAmount {
			t.Errorf("This should have changed")
			return
		} else {
			lastAmount = count
		}
		count--
	}
}

func TestString(t *testing.T) {
	c := getCQF()

	item := []byte("lol")

	c.Insert(item, 1)
	if count := c.Count(item); count != 1 {
		t.Errorf("Expected something for item: %s", item)
	}
}

func TestAlottMixed(t *testing.T) {
	c := getCQF()

	hash := uint32(rand.Int31())
	count := rand.Int31n(10000) + 1000
	totalAmount := uint64(0)

	for count > 0 {
		amount := uint64(1)
		if count%2 == 1 {
			amount = uint64(rand.Int63n(100))
		}

		totalAmount += amount
		c.InsertHash(hash, amount)
		count--
	}

	if got := c.CountHash(hash); got != totalAmount {
		t.Errorf("TestAlottMixed - got: %d, expected: %d", got, totalAmount)
	}
}

func TestMultipleAmounts(t *testing.T) {
	c := getCQF()

	hash := uint32(64)
	amounts := []uint64{32, 2, 1, 1, 10, 20, 4, 4, 1, 1, 32, 1, 4}
	total := uint64(0)
	for _, amount := range amounts {
		c.InsertHash(hash, amount)
		total += amount
		if got := c.CountHash(hash); got != total {
			t.Errorf("got: %d", got)
		}

	}
}

func TestMultiSimple(t *testing.T) {
	c := getCQF()

	hash := uint32(4)
	total := uint64(0)
	count := 100
	for count < 10 {
		amount := uint64(1)
		if count%2 == 1 {
			amount = uint64(rand.Int63n(100))
		}

		c.InsertHash(hash, amount)
		total += amount
		if count := c.CountHash(hash); count != total {
			t.Errorf("error, c: %d, expected: %d", count, total)
			return
		}

		count++
	}
}

func TestAlottOfTheSameMulti(t *testing.T) {
	c := getCQF()

	hash := uint32(4)
	want := uint64(0)
	count := 10 //rand.Int31n(1234) + 100

	for count > 0 {
		amount := uint64(rand.Int63n(1024))
		c.InsertHash(hash, amount)
		want += amount
		count--
	}

	if got := c.CountHash(hash); got != want {
		t.Errorf("got: %d, expected: %d", got, want)
	}
}

func TestCountOkThenInsertAndCountStillOk(t *testing.T) {

	c := getCQF()

	c.InsertHash(uint32(14), 6)
	if got := c.CountHash(uint32(14)); got != uint64(6) {
		t.Errorf("got: %d, expected: %d", got, uint64(6))

	}
	fmt.Println("ok")

	c.InsertHash(uint32(1), 5)

	fmt.Println("ok")
	if got := c.CountHash(uint32(14)); got != uint64(6) {
		t.Errorf("got: %d, expected: %d", got, uint64(6))

	}

	c.InsertHash(uint32(15), 5)

	fmt.Println("ok")
	if got := c.CountHash(uint32(14)); got != uint64(6) {
		t.Errorf("got: %d, expected: %d", got, uint64(6))

	}
}

func TestFull(t *testing.T) {
	c := getCQF()

	checkVals := map[uint32]uint64{}

	count := uint32(rand.Int31n(10000) + 10000)

	for count > 0 {
		hash := uint32(rand.Int63n(math.MaxUint32-2) + 1)
		amount := uint64(rand.Int63n(1024*1024) + 1)
		c.InsertHash(uint32(hash), amount)

		val := checkVals[hash]
		val += amount
		checkVals[hash] = val

		count--
	}

	off := 0
	for hash, want := range checkVals {
		if got := c.CountHash(hash); got != want && got == 0 {
			off++
		}
	}
}

func BenchmarkFull(b *testing.B) {
	c := getCQF()
	for n := 0; n < b.N; n++ {
		hash := uint32(rand.Int63n(math.MaxUint32-2) + 1)
		amount := uint64(rand.Int63n(1024*1024) + 1)
		c.InsertHash(uint32(hash), amount)
	}
}

func init() {
	rand.Seed(time.Now().Unix())
	fmt.Println("")
}
