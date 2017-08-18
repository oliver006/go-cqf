package cqf

import (
	"fmt"
	"math"

	"github.com/spaolacci/murmur3"
	"github.com/tmthrgd/go-popcount"
)

const (
	BitsPerSlot           = 8
	BlockOffsetBits       = 6
	SlotsPerBlock         = 1 << BlockOffsetBits
	MetadataWordsPerBlock = (SlotsPerBlock + 63) / 64
)

type block struct {
	offset    uint16
	occupieds [MetadataWordsPerBlock]uint64
	runends   [MetadataWordsPerBlock]uint64

	slots [SlotsPerBlock]uint8
}

type CQF struct {
	xnSlots uint64

	bitsPerSlot       uint64
	bitsPerSlotMasked uint64

	nelts           uint64
	ndistinct_elts  uint64
	noccupied_slots uint64

	blocks []*block
}

func NewCQF() (*CQF, error) {

	qbits := uint64(24)

	keyBits := uint64(qbits + 8)
	// todo: this correct?
	numSlots := uint64(1 << qbits)
	//keyBits := uint64(32)
	//numSlots := uint64(1 << (keyBits - 8))

	c := CQF{
		xnSlots: numSlots + 10*uint64(math.Sqrt(float64(numSlots))),
	}

	for numSlots > 1 {
		if keyBits == 0 {
			return nil, fmt.Errorf("c.KeyRemainderBits == 0")
		}
		keyBits -= 1
		numSlots >>= 1
	}
	c.bitsPerSlot = keyBits
	c.bitsPerSlotMasked = bitmask(c.bitsPerSlot)

	if c.bitsPerSlot != BitsPerSlot {
		msg := fmt.Sprintf("c.bitsPerSlot != %d, got: %d", BitsPerSlot, c.bitsPerSlot)
		return nil, fmt.Errorf(msg)
	}

	numBlocks := (c.xnSlots + SlotsPerBlock - 1) / SlotsPerBlock
	c.blocks = make([]*block, numBlocks)
	for pos := range c.blocks {
		c.blocks[pos] = &block{}
	}

	return &c, nil
}

func hashItem(item []byte) uint32 {
	hasher := murmur3.New32()
	hasher.Write(item)
	return uint32(hasher.Sum32())
}

func (c *CQF) Insert(item []byte, count uint64) {
	c.InsertHash(hashItem(item), count)
}

func (c *CQF) InsertHash(hash uint32, count uint64) {
	//	fmt.Printf("InsertHash(%d, %d) \n", hash, count)
	if count == 1 {
		c.insert1(uint64(hash))
	} else {
		c.insert(uint64(hash), count)
	}
}

func (c *CQF) Count(item []byte) uint64 {
	return c.countHash(uint64(hashItem(item)))
}

func (c *CQF) CountHash(hash uint32) uint64 {
	return c.countHash(uint64(hash))
}

func (c *CQF) Remove(item []byte, count uint64) {
}

func (c *CQF) RemoveHash(hash uint32, count uint64) {
}

func (c *CQF) DeleteItem(item []byte) {
}

/*
 * Implementation
 *
 *
 */
func (c *CQF) offsetLowerBound(slotIndex uint64) uint64 {
	var res uint64

	blockIdx := slotIndex / SlotsPerBlock

	b := c.blocks[blockIdx]

	slotOffset := slotIndex % SlotsPerBlock
	boffset := uint64(b.offset)
	occupieds := b.occupieds[0] & bitmask(slotOffset+1)

	if boffset <= slotOffset {
		runends := (b.runends[0] & bitmask(slotOffset)) >> boffset
		res = popcount.Count64(occupieds) - popcount.Count64(runends)
	} else {
		res = boffset - slotOffset + popcount.Count64(occupieds)
	}
	return res
}

func (c *CQF) isEmpty(slotIndex uint64) bool {
	return c.offsetLowerBound(slotIndex) == 0
}

func (c *CQF) METADATA_WORD_runends(slotIndex uint64) *uint64 {
	b := c.blocks[slotIndex/SlotsPerBlock]

	return &(b.runends[((slotIndex)%SlotsPerBlock)/64])
}

func (c *CQF) METADATA_WORD_occupieds(slotIndex uint64) *uint64 {
	b := c.blocks[slotIndex/SlotsPerBlock]

	return &(b.occupieds[((slotIndex)%SlotsPerBlock)/64])
}

func (c *CQF) isRunEnd(index uint64) bool {
	pos := ((index % SlotsPerBlock) % 64)
	res := ((*(c.METADATA_WORD_runends(index)) >> pos) & uint64(1))
	return res == 1
}

func (c *CQF) isOccupied(index uint64) bool {
	return ((*(c.METADATA_WORD_occupieds(index)) >> ((index % SlotsPerBlock) % 64)) & uint64(1)) == 1
}

func (c *CQF) setSlot(index uint64, val uint64) {
	newVal := uint8(val & bitmask(c.bitsPerSlot))

	blockIdx := index / SlotsPerBlock
	slotIdx := index % SlotsPerBlock

	//	fmt.Printf("set_slot( %2d %2d %2d ) --> %d \n", index, blockIdx, slotIdx, newVal)
	c.blocks[blockIdx].slots[slotIdx] = newVal
}

func (c *CQF) getSlot(index uint64) uint64 {
	blockIdx := index / SlotsPerBlock
	slotIdx := index % SlotsPerBlock

	//	fmt.Printf("get_slot( %2d %2d %2d )", index, blockIdx, slotIdx)
	res := uint64(c.blocks[blockIdx].slots[slotIdx])
	//	fmt.Printf(" --> %d \n", res)

	return res
}

func (c *CQF) splitHash(hash uint64) (remainder uint64, bucketIndex uint64, bucketBlockOffset uint64) {
	remainder = hash & c.bitsPerSlotMasked
	bucketIndex = hash >> c.bitsPerSlot
	bucketBlockOffset = bucketIndex % SlotsPerBlock
	return
}

func (c *CQF) insert(hash uint64, count uint64) {
	hash_remainder, hashBucketIndex, hashBucketBlockOffset := c.splitHash(hash)

	if c.isEmpty(hashBucketIndex) {
		val := c.METADATA_WORD_runends(hashBucketIndex)
		*val = *val | uint64(1)<<(hashBucketBlockOffset%64)

		c.setSlot(hashBucketIndex, hash_remainder)
		c.noccupied_slots++

		// METADATA_WORD(qf, occupieds, hash_bucket_index) |= 1ULL << (hash_bucket_block_offset % 64);
		val = c.METADATA_WORD_occupieds(hashBucketIndex)
		*val = *val | uint64(1)<<(hashBucketBlockOffset%64)
		c.nelts += 1

		c.ndistinct_elts++

		if count > 1 {
			c.insert(hash, count-1)
		}

	} else {
		// non-empty slot

		new_values := make([]uint64, 67)
		var runstart_index uint64

		if hashBucketIndex != 0 {
			runstart_index = c.runEnd(hashBucketIndex-1) + 1
		}

		if !c.is_occupied(hashBucketIndex) {

			pos := c.encode_counter(hash_remainder, count, &new_values)

			new_values := new_values[pos:]
			c.insert_replace_slots_and_shift_remainders_and_runends_and_offsets(
				0,
				hashBucketIndex,
				runstart_index,
				&new_values,
				67-pos,
				0)
			c.ndistinct_elts++

		} else {

			current_end, current_remainder, current_count := c.decode_counter(runstart_index)

			for current_remainder < hash_remainder && !c.isRunEnd(current_end) {
				runstart_index = current_end + 1
				current_end, current_remainder, current_count = c.decode_counter(runstart_index)
			}

			// If we reached the end of the run w/o finding a counter for this remainder,
			//	 then append a counter for this remainder to the run.
			if current_remainder < hash_remainder {
				pos := c.encode_counter(hash_remainder, count, &new_values)
				new_values := new_values[pos:]
				c.insert_replace_slots_and_shift_remainders_and_runends_and_offsets(
					1,
					hashBucketIndex,
					current_end+1,
					&new_values,
					67-pos,
					0)
				c.ndistinct_elts++

				c.ndistinct_elts++

				// Found a counter for this remainder.  Add in the new count.
			} else if current_remainder == hash_remainder {
				pos := c.encode_counter(hash_remainder, current_count+count, &new_values)
				new_values := new_values[pos:]
				c.insert_replace_slots_and_shift_remainders_and_runends_and_offsets(
					1,
					hashBucketIndex,
					runstart_index,
					&new_values,
					67-pos,
					current_end-runstart_index+1)

				// No counter for this remainder, but there are larger
				//	 remainders, so we're not appending to the bucket.
			} else {

				pos := c.encode_counter(hash_remainder, count, &new_values)
				new_values := new_values[pos:]
				c.insert_replace_slots_and_shift_remainders_and_runends_and_offsets(
					2,
					hashBucketIndex,
					runstart_index,
					&new_values,
					67-pos,
					0)
				c.ndistinct_elts++
			}
		}
	}

	// METADATA_WORD(qf, occupieds, hashBucketIndex) |= 1ULL << (hashBucketBlockOffset % 64);
	val := c.METADATA_WORD_occupieds(hashBucketIndex)
	*val = *val | uint64(1)<<(hashBucketBlockOffset%64)

	c.nelts += count
}

func (c *CQF) find_next_n_empty_slots(from, n uint64, res *[]uint64) {
	for n > 0 {
		n--
		(*res)[n] = c.findFirstEmptySlot(from)
		from = (*res)[n] + 1
	}
}

func (c *CQF) shift_slots(first, last, distance uint64) {
	if distance == 1 {
		c.shiftRemainders(first, last+1)
		return
	}

	for tmp := int(last); tmp >= int(first); tmp-- {
		i := uint64(tmp)
		c.setSlot(i+distance, c.getSlot(i))
	}
}

func (c *CQF) insert_replace_slots_and_shift_remainders_and_runends_and_offsets(
	operation int,
	bucket_index, overwrite_index uint64,
	remainders *[]uint64,
	total_remainders uint64,
	noverwrites uint64) {

	empties := make([]uint64, 67)
	ninserts := total_remainders - noverwrites
	insert_index := uint64(overwrite_index + noverwrites)
	if ninserts > 0 {
		// First, shift things to create n empty spaces where we need them.
		c.find_next_n_empty_slots(insert_index, ninserts, &empties)

		for i := uint64(0); i < ninserts-1; i++ {
			c.shift_slots(empties[i+1]+1, empties[i]-1, i+1)
		}
		if ninserts > 0 {
			c.shift_slots(insert_index, empties[ninserts-1]-1, ninserts)
		}

		for i := uint64(0); i < ninserts-1; i++ {
			c.shiftRunEnds(empties[i+1]+1, empties[i]-1, i+1)
		}

		if ninserts > 0 {
			c.shiftRunEnds(insert_index, empties[ninserts-1]-1, ninserts)
		}

		for i := noverwrites; i < total_remainders-1; i++ {
			//				METADATA_WORD(qf, runends, overwrite_index + i) &= ~(1ULL << (((overwrite_index + i) % SLOTS_PER_BLOCK) % 64))
			val := c.METADATA_WORD_runends(overwrite_index + i)
			*val = *val & (^(uint64(1) << (((overwrite_index + i) % SlotsPerBlock) % 64)))
		}

		switch operation {
		case 0: // insert into empty bucket
			// assert (noverwrites == 0);
			// METADATA_WORD(qf, runends, overwrite_index + total_remainders - 1) |= 1ULL << (((overwrite_index + total_remainders - 1) % SLOTS_PER_BLOCK) % 64);
			val := c.METADATA_WORD_runends(overwrite_index + total_remainders - 1)
			*val = *val | (uint64(1) << (((overwrite_index + total_remainders - 1) % SlotsPerBlock) % 64))
		case 1: // append to bucket
			//METADATA_WORD(qf, runends, overwrite_index + noverwrites - 1)      &= ~(1ULL << (((overwrite_index + noverwrites - 1) % SLOTS_PER_BLOCK) % 64));
			val := c.METADATA_WORD_runends(overwrite_index + noverwrites - 1)
			*val = *val & (^(uint64(1) << (((overwrite_index + noverwrites - 1) % SlotsPerBlock) % 64)))

			//METADATA_WORD(qf, runends, overwrite_index + total_remainders - 1) |= 1ULL << (((overwrite_index + total_remainders - 1) % SLOTS_PER_BLOCK) % 64);
			val = c.METADATA_WORD_runends(overwrite_index + total_remainders - 1)
			*val = *val | (uint64(1) << (((overwrite_index + total_remainders - 1) % SlotsPerBlock) % 64))
		case 2: // insert into bucket
			//METADATA_WORD(qf, runends, overwrite_index + total_remainders - 1) &= ~(1ULL << (((overwrite_index + total_remainders - 1) % SLOTS_PER_BLOCK) % 64));
			val := c.METADATA_WORD_runends(overwrite_index + total_remainders - 1)
			*val = *val & (^(uint64(1) << (((overwrite_index + total_remainders - 1) % SlotsPerBlock) % 64)))
		default:
			panic("Invalid operation ")
		}

		if ninserts > 0 {
			for i := bucket_index/SlotsPerBlock + 1; i <= empties[ninserts-1]/SlotsPerBlock; i++ {
				if uint64(c.blocks[i].offset) < bitmask(uint64(8*2)) {
					c.blocks[i].offset += uint16(ninserts)
				}
			}
			for j := uint64(0); j < ninserts-1; j++ {
				for i := empties[ninserts-j-1]/SlotsPerBlock + 1; i <= empties[ninserts-j-2]/SlotsPerBlock; i++ {
					if uint64(c.blocks[i].offset) < bitmask(uint64(8*2)) {
						c.blocks[i].offset += uint16(ninserts - j - 1)
					}
				}
			}
		}
	}

	for i := uint64(0); i < total_remainders; i++ {
		c.setSlot(overwrite_index+i, (*remainders)[i])
	}

	c.noccupied_slots += uint64(ninserts)
}

func (c *CQF) encode_counter(remainder, counter uint64, slots *[]uint64) uint64 {
	digit := remainder
	base := (uint64(1) << c.bitsPerSlot) - 1
	p := uint64(len(*slots))

	if counter == 0 {
		return p
	}

	p--
	(*slots)[p] = remainder

	switch counter {
	case 1:
		return p
	case 2:
		p--
		(*slots)[p] = remainder
		return p
	case 3:
		if remainder == 0 {
			p--
			(*slots)[p] = remainder
			p--
			(*slots)[p] = remainder

		} else {
			p--
			(*slots)[p] = 0
			p--
			(*slots)[p] = remainder
		}
		return p
	}

	if remainder == 0 {
		p--
		(*slots)[p] = remainder
		counter -= 4
	} else {
		base--
		counter -= 3
	}

	for {
		digit = counter % base
		digit++ /* Zero not allowed */

		/*
			// r: 40  d:  2 --> false
			// r: 64  d: 64 --> true
			if (remainder == 0) || ((remainder > 0 && digit > 0) && (remainder == 1)) {
		*/
		if (remainder > 0) && (digit >= remainder) {
			digit++ /* Cannot overflow since digit is mod 2^r-2 */
		}
		p--
		(*slots)[p] = digit

		counter /= base
		if counter == 0 {
			break
		}
	}

	if (remainder > 0) && (digit >= remainder) {
		p--
		(*slots)[p] = 0
	}

	p--
	(*slots)[p] = remainder

	return p
}

func (c *CQF) insert1(hash uint64) {
	hash_remainder, hashBucketIndex, hashBucketBlockOffset := c.splitHash(hash)

	if c.isEmpty(hashBucketIndex) {
		val := c.METADATA_WORD_runends(hashBucketIndex)
		*val = *val | uint64(1)<<(hashBucketBlockOffset%64)

		c.setSlot(hashBucketIndex, hash_remainder)

		c.noccupied_slots++
		c.ndistinct_elts++

	} else {
		runend_index := c.runEnd(hashBucketIndex)
		operation := 0
		insert_index := runend_index + 1
		new_value := hash_remainder

		var runstart_index uint64
		if hashBucketIndex != 0 {
			runstart_index = c.runEnd(hashBucketIndex-1) + 1
		}

		if c.isOccupied(hashBucketIndex) {

			current_remainder := c.getSlot(runstart_index)
			zero_terminator := runstart_index

			// The counter for 0 is special.
			if current_remainder == 0 {
				t := runstart_index + 1
				for t < runend_index && c.getSlot(t) != 0 {
					t++
				}

				if t < runend_index && c.getSlot(t+1) == 0 {
					zero_terminator = t + 1 // Three or more 0s
				} else if runstart_index < runend_index && c.getSlot(runstart_index+1) == 0 {
					zero_terminator = runstart_index + 1 // Exactly two 0s
				}
				// Otherwise, exactly one 0 (i.e. zero_terminator == runstart_index)

				// May read past end of run, but that's OK because loop below can handle that
				if hash_remainder != 0 {
					runstart_index = zero_terminator + 1
					current_remainder = c.getSlot(runstart_index)
				}
			}
			for current_remainder < hash_remainder && runstart_index <= runend_index {
				/* If this remainder has an extended counter, skip over it. */
				if runstart_index < runend_index && c.getSlot(runstart_index+1) < current_remainder {
					runstart_index = runstart_index + 2
					for c.getSlot(runstart_index) != current_remainder {
						runstart_index++
					}
					runstart_index++

					/* This remainder has a simple counter. */
				} else {
					runstart_index++
				}

				/* This may read past the end of the run, but the while loop
				condition will prevent us from using the invalid result in
				that case. */
				current_remainder = c.getSlot(runstart_index)
			}

			// If this is the first time we've inserted the new remainder,
			//	 and it is larger than any remainder in the run.
			if runstart_index > runend_index {
				operation = 1
				insert_index = runstart_index
				new_value = hash_remainder
				c.ndistinct_elts++

				// This is the first time we're inserting this remainder, but
				//	 there are larger remainders already in the run.
			} else if current_remainder != hash_remainder {
				operation = 2 // Inserting
				insert_index = runstart_index
				new_value = hash_remainder
				c.ndistinct_elts++

				// Cases below here: we're incrementing the (simple or
				//	 extended) counter for this remainder.

				// If there's exactly one instance of this remainder.
			} else if runstart_index == runend_index ||
				(hash_remainder > 0 && c.getSlot(runstart_index+1) > hash_remainder) ||
				(hash_remainder == 0 && zero_terminator == runstart_index) {
				operation = 2 // Insert
				insert_index = runstart_index
				new_value = hash_remainder

				// If there are exactly two instances of this remainder.
			} else if (hash_remainder > 0 && c.getSlot(runstart_index+1) == hash_remainder) ||
				(hash_remainder == 0 && zero_terminator == runstart_index+1) {
				operation = 2 // Insert
				insert_index = runstart_index + 1
				new_value = 0

				// Special case for three 0s
			} else if hash_remainder == 0 && zero_terminator == runstart_index+2 {
				operation = 2 // Insert
				insert_index = runstart_index + 1
				new_value = 1

				// There is an extended counter for this remainder.
			} else {
				// Move to the LSD of the counter.
				insert_index = runstart_index + 1

				for c.getSlot(insert_index+1) != hash_remainder {
					insert_index++
				}

				// Increment the counter.
				var digit, carry uint64
				for {
					carry = 0
					digit = c.getSlot(insert_index)
					// Convert a leading 0 (which is special) to a normal encoded digit
					if digit == 0 {
						digit++
						if digit == current_remainder {
							digit++
						}
					}

					// Increment the digit
					digit = (digit + 1) & c.bitsPerSlotMasked

					// Ensure digit meets our encoding requirements
					if digit == 0 {
						digit++
						carry = 1
					}
					if digit == current_remainder {
						digit = (digit + 1) & c.bitsPerSlotMasked
					}
					if digit == 0 {
						digit++
						carry = 1
					}

					c.setSlot(insert_index, digit)
					insert_index--
					if !(insert_index > runstart_index && carry != 0) {
						break
					}
				}

				// If the counter needs to be expanded.
				if insert_index == runstart_index && (carry > 0 || (current_remainder != 0 && digit >= current_remainder)) {
					operation = 2 // insert
					insert_index = runstart_index + 1
					if carry == 0 {
						// To prepend a 0 before the counter if the MSD is greater than the rem
						new_value = 0
					} else { // if (carry) {			// Increment the new value because we don't use 0 to encode counters
						new_value = 2
						// If the rem is greater than or equal to the new_value then fail
						if current_remainder > 0 {
							if !(new_value < current_remainder) {
								panic("!(new_value < current_remainder)")
							}
						}
					}
				} else {
					operation = -1
				}
			}
		}

		if operation >= 0 {
			empty_slot_index := c.findFirstEmptySlot(runend_index + 1)

			c.shiftRemainders(insert_index, empty_slot_index)
			c.setSlot(insert_index, new_value)
			c.shiftRunEnds(insert_index, empty_slot_index-1, 1)

			switch operation {
			case 0:
				// METADATA_WORD(qf, runends, insert_index)   |= 1ULL << ((insert_index % SLOTS_PER_BLOCK) % 64);

				val := c.METADATA_WORD_runends(insert_index)
				*val = *val | uint64(1)<<((insert_index%SlotsPerBlock)%64)
			case 1:
				// METADATA_WORD(qf, runends, insert_index-1) &= ~(1ULL << (((insert_index-1) % SLOTS_PER_BLOCK) % 64));
				val := c.METADATA_WORD_runends(insert_index - 1)
				*val = *val & ^(uint64(1) << (((insert_index - 1) % SlotsPerBlock) % 64))

				//METADATA_WORD(qf, runends, insert_index)   |= 1ULL << ((insert_index % SLOTS_PER_BLOCK) % 64);
				val = c.METADATA_WORD_runends(insert_index)
				*val = *val | uint64(1)<<((insert_index%SlotsPerBlock)%64)
			case 2:
				// METADATA_WORD(qf, runends, insert_index)   &= ~(1ULL << ((insert_index % SLOTS_PER_BLOCK) % 64));
				val := c.METADATA_WORD_runends(insert_index)

				*val = *val & ^(uint64(1) << (((insert_index) % SlotsPerBlock) % 64))
			default:
				panic(fmt.Sprintf("Invalid operation %d\n", operation))
			}

			/*
			 * Increment the offset for each block between the hash bucket index
			 * and block of the empty slot
			 * */
			for i := hashBucketIndex/SlotsPerBlock + 1; i <= empty_slot_index/SlotsPerBlock; i++ {
				// hint: 8 * 2 --> 2 = sizeof(offset)
				if uint64(c.blocks[i].offset) < bitmask(8*2) {
					c.blocks[i].offset++
				}
				//assert(get_block(qf, i)->offset != 0);
			}

			c.noccupied_slots++
		}
	}

	// METADATA_WORD(qf, occupieds, hashBucketIndex) |= 1ULL << (hashBucketBlockOffset % 64);
	val := c.METADATA_WORD_occupieds(hashBucketIndex)
	*val = *val | uint64(1)<<(hashBucketBlockOffset%64)

	c.nelts++
}

func (c *CQF) block_offset(blockidx uint64) uint64 {
	// hint: 8 * 2 --> sizeof(block.offset)
	if uint64(c.blocks[blockidx].offset) < bitmask(8*2) {
		return uint64(c.blocks[blockidx].offset)
	}
	return c.runEnd(SlotsPerBlock*blockidx-1) - SlotsPerBlock*blockidx + 1
}

func (c *CQF) runEnd(hashBucketIndex uint64) uint64 {
	bucket_block_index := hashBucketIndex / SlotsPerBlock
	bucket_intrablock_offset := hashBucketIndex % SlotsPerBlock
	bucket_blocks_offset := c.block_offset(bucket_block_index)
	b := c.blocks[bucket_block_index]
	bucket_intrablock_rank := bitrank(b.occupieds[0], bucket_intrablock_offset)

	if bucket_intrablock_rank == 0 {
		if bucket_blocks_offset <= bucket_intrablock_offset {
			return hashBucketIndex
		} else {
			return SlotsPerBlock*bucket_block_index + bucket_blocks_offset - 1
		}
	}

	runend_block_index := bucket_block_index + bucket_blocks_offset/SlotsPerBlock
	runend_ignore_bits := bucket_blocks_offset % SlotsPerBlock
	runend_rank := bucket_intrablock_rank - 1

	//	uint64_t runend_block_offset = bitselectv(get_block(qf, runend_block_index)->runends[0], runend_ignore_bits, runend_rank);
	runend_block_offset := bitselectv(c.blocks[runend_block_index].runends[0], runend_ignore_bits, runend_rank)
	if runend_block_offset == SlotsPerBlock {
		if bucket_blocks_offset == 0 && bucket_intrablock_rank == 0 {
			/* The block begins in empty space, and this bucket is in that region of empty space */
			return hashBucketIndex
		} else {

			for {
				// runend_rank        -= popcntv(get_block(qf, runend_block_index)->runends, METADATA_WORDS_PER_BLOCK, runend_ignore_bits);
				runend_rank -= popcntv(c.blocks[runend_block_index].runends[0], runend_ignore_bits)
				runend_block_index++
				runend_ignore_bits = 0
				// runend_block_offset = bitselectv(get_block(qf, runend_block_index)->runends, METADATA_WORDS_PER_BLOCK, runend_ignore_bits, runend_rank);
				runend_block_offset = bitselectv(c.blocks[runend_block_index].runends[0], runend_ignore_bits, runend_rank)

				if runend_block_offset != SlotsPerBlock {
					break
				}
			}
		}
	}
	runend_index := SlotsPerBlock*runend_block_index + runend_block_offset

	if runend_index < hashBucketIndex {
		return hashBucketIndex
	} else {
		return runend_index
	}
}

func (c *CQF) decode_counter(index uint64) (last_el, remainder, count uint64) {
	remainder = c.getSlot(index)
	orgRemainder := remainder

	if c.isRunEnd(index) {
		count = 1
		last_el = index
		return
	}

	var digit = c.getSlot(index + 1)
	if c.isRunEnd(index + 1) {

		count = 1
		last_el = index

		if digit == orgRemainder {
			count = 2
			last_el++
		}
		return
	}

	if orgRemainder > 0 && digit >= orgRemainder {
		count = 1
		last_el = index

		if digit == orgRemainder {
			count = 2
			last_el++
		}
		return
	}

	if orgRemainder > 0 && digit == 0 && c.getSlot(index+2) == orgRemainder {
		count = 3
		last_el = index + 2
	}

	if orgRemainder == 0 && digit == 0 {
		if c.getSlot(index+2) == 0 {
			count = 3
			last_el = index + 2
			return
		} else {
			count = 2
			last_el = index + 1
			return
		}
	}

	cnt := uint64(0)
	base := (uint64(1) << c.bitsPerSlot) - 1
	if orgRemainder > 0 {
		base--
	}

	end := index + 1
	for digit != orgRemainder && !c.isRunEnd(end) {
		if digit > orgRemainder {
			digit--
		}
		if digit != 0 && orgRemainder != 0 {
			digit--
		}
		cnt = cnt*base + digit

		end++
		digit = c.getSlot(end)
	}

	if orgRemainder > 0 {
		count = cnt + 3
		last_el = end
		return
	}

	if c.isRunEnd(end) || c.getSlot(end+1) != 0 {
		count = 1
		last_el = index
		return
	}

	count = cnt + 4
	last_el = end + 1

	return
}

func (c *CQF) countHash(hash uint64) uint64 {
	hashRemainder, hashBucketIndex, _ := c.splitHash(hash)
	if !c.isOccupied(hashBucketIndex) {
		return 0
	}

	var runstart_index uint64
	if hashBucketIndex != 0 {
		runstart_index = c.runEnd(hashBucketIndex-1) + 1
		if runstart_index < hashBucketIndex {
			runstart_index = hashBucketIndex
		}
	}

	for {
		current_end, current_remainder, current_count := c.decode_counter(runstart_index)
		if current_remainder == hashRemainder {
			return current_count
		}
		runstart_index = current_end + 1
		if c.isRunEnd(current_end) {
			break
		}
	}

	return 0
}

func (c *CQF) is_occupied(index uint64) bool {
	// (METADATA_WORD(qf, occupieds, index) >> ((index % SLOTS_PER_BLOCK) % 64)) & 1ULL;
	return (*c.METADATA_WORD_occupieds(index)>>((index%SlotsPerBlock)%64))&1 != 0
}

func (c *CQF) findFirstEmptySlot(from uint64) uint64 {
	for {
		t := c.offsetLowerBound(from)
		if t == 0 {
			break
		}
		from = from + t
	}
	return from
}

func (c *CQF) shiftRemainders(start_index, empty_index uint64) {
	start_block := start_index / SlotsPerBlock
	start_offset := start_index % SlotsPerBlock
	empty_block := empty_index / SlotsPerBlock
	empty_offset := empty_index % SlotsPerBlock

	//	assert (start_index <= empty_index && empty_index < qf->xnslots);

	for start_block < empty_block {

		// todo: optimize by doing this backwards so to not override still needed bytes
		var newSlots [SlotsPerBlock]uint8
		for pos, val := range c.blocks[empty_block].slots {
			newSlots[pos] = val
		}
		pos := uint64(0)
		for pos < empty_offset {
			newSlots[pos+1] = c.blocks[empty_block].slots[pos]
			pos++
		}

		newSlots[0] = c.blocks[empty_block-1].slots[SlotsPerBlock-1]

		c.blocks[empty_block].slots = newSlots

		empty_block--
		empty_offset = SlotsPerBlock - 1
	}
	// todo: optimize by doing this backwards so to not override still needed bytes
	var newSlots [SlotsPerBlock]uint8
	for pos, val := range c.blocks[empty_block].slots {
		newSlots[pos] = val
	}

	for pos := 0; pos < int(empty_offset-start_offset); {
		x := int(start_offset) + pos
		newSlots[x+1] = c.blocks[empty_block].slots[x]
		pos++
	}

	c.blocks[empty_block].slots = newSlots
}

func (c *CQF) shiftRunEnds(first, last, distance uint64) {
	if (first > last) && ((first - last) > 10) {
		panic("(first - last) > 10")
	}

	if !(last < c.xnSlots && distance < 64) {
		panic("!(last < qf->xnslots && distance < 64)")
	}
	first_word := first / 64
	bstart := first % 64
	last_word := (last + distance + 1) / 64
	bend := (last + distance + 1) % 64

	if last_word < first_word {
		panic("last_word < first_word")
	}

	if last_word != first_word {

		val := c.METADATA_WORD_runends(64 * last_word)
		val2 := c.METADATA_WORD_runends(64 * (last_word - 1))
		*val = shiftIntoB(*val2, *val, 0, bend, distance)

		bend = 64
		last_word--
		for last_word != first_word {
			val := c.METADATA_WORD_runends(64 * last_word)
			val2 := c.METADATA_WORD_runends(64 * (last_word - 1))
			*val = shiftIntoB(*val2, *val, 0, bend, distance)
			last_word--

		}
	}

	val := c.METADATA_WORD_runends(64 * last_word)
	*val = shiftIntoB(0, *val, bstart, bend, distance)

}
