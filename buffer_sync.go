// SPDX-FileCopyrightText: 2025 Kiril Strezikozin
//
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ring

import (
	"io"
	"sync"
	"sync/atomic"
)

const (
	canWriteElement = uint32(iota)
	canReadElement
)

type element[V any] struct {
	can   atomic.Uint32
	value V
}

// SyncBuffer implements [Bufferer], a circular (ring) fixed-size buffer with
// constant time O(1) element adding and removing operations. SyncBuffer is
// safe for concurrent use. Use [NewSync] to create it.
//
// SyncBuffer with element type `byte` implements [io.ReadWriter] interface.
type SyncBuffer[V any] struct {
	count atomic.Uintptr
	write int
	read  int

	pullMu sync.Mutex
	pushMu sync.Mutex

	data []element[V]
}

// TODO: benchmark, docs, reference, go report, coverage, CI, branch protection.

// NewSync allocates and returns a new thread-safe, fixed-size circular (ring)
// buffer that fits n elements in total.
func NewSync[V any](n int) *SyncBuffer[V] {
	return &SyncBuffer[V]{
		data: make([]element[V], n),
	}
}

// NewSyncFrom allocates and returns a new thread-safe, circular (ring) buffer
// with contents pre-populated from data. Data is assumed to already contain
// elements, as such [SyncBuffer] will store the next element to push at
// len(data), and wrap to the beginning when cap(data) is reached.
// Pulling will start from the beginning.
//
// The returned [SyncBuffer] does not retain data.
//
// See also: [NewSync].
func NewSyncFrom[V any](data []V) *SyncBuffer[V] {
	rb := &SyncBuffer[V]{
		write: len(data),
		data:  make([]element[V], cap(data)),
	}

	rb.count.Store(uintptr(len(data)))
	for i, v := range data {
		rb.data[i].value = v
	}

	return rb
}

// Count returns the number of elements currently stored in the ring buffer.
func (rb *SyncBuffer[V]) Count() int {
	if len(rb.data) == 0 { // Fast path.
		return 0
	}

	return int(rb.count.Load()) // Slow path.
}

// Len returns the size of the ring buffer's data. This value is the maximum
// number of elements the buffer can hold.
func (rb *SyncBuffer[V]) Len() int {
	// rb.data is never re-sliced, thus this value never changes.
	return len(rb.data)
}

// Offer tries to insert a new element into the ring buffer and returns true
// if the buffer is not full and insertion succeeds. Otherwise, Offer does not
// block and immediately returns false.
func (rb *SyncBuffer[V]) Offer(value V) bool {
	rb.pushMu.Lock()

	newWrite := rb.write + 1
	if newWrite == len(rb.data) {
		newWrite = 0
	}

	if rb.data[rb.write].can.Load() == canWriteElement {
		rb.data[rb.write].value = value
		rb.data[rb.write].can.Store(canReadElement)
		rb.write = newWrite

		rb.pushMu.Unlock()
		rb.count.Add(1)
		return true
	}

	// Buffer is full, cannot insert.
	rb.pushMu.Unlock()
	return false
}

// Push inserts a new element into the ring buffer, blocking if the buffer is
// full. It blocks indefinitely if the buffer is full and no reader ever
// reads an element from the buffer. For more control over insertion,
// see [SyncBuffer.Offer] and [SyncBuffer.PushWithContext].
func (rb *SyncBuffer[V]) Push(value V) {
	rb.pushMu.Lock()

	newWrite := rb.write + 1
	if newWrite == len(rb.data) {
		newWrite = 0
	}

	// If the buffer is not full or someone has concurrently pulled off an
	// element, we will not block. Otherwise, only the active pusher spins,
	// others queue up and put to sleep because pushMu is locked, see
	// [sync.Mutex.Lock]. We could consider putting waiting go-routines
	// to sleep here directly in the future.

	for {
		if rb.data[rb.write].can.Load() == canWriteElement {
			rb.data[rb.write].value = value
			rb.data[rb.write].can.Store(canReadElement)
			rb.write = newWrite

			rb.pushMu.Unlock()
			rb.count.Add(1)
			return
		}
	}
}

// Poll tests if there is an element to read in the ring buffer. If the buffer
// is empty, Poll returns the element's zero value and false. Otherwise, it
// pulls the element and returns it with true.
//
// This function behaves similarly to the two-value assignment testing for the
// existence of a key in a map. Poll does not block and returns false
// immediately if the buffer is empty.
func (rb *SyncBuffer[V]) Poll() (V, bool) {
	var value V

	rb.pullMu.Lock()

	newRead := rb.read + 1
	if newRead == len(rb.data) {
		newRead = 0
	}

	if rb.data[rb.read].can.Load() == canReadElement {
		value = rb.data[rb.read].value
		rb.data[rb.read].can.Store(canWriteElement)
		rb.read = newRead

		rb.pullMu.Unlock()
		rb.count.Add(^uintptr(0))
		return value, true
	}

	// Buffer is empty.
	rb.pullMu.Unlock()
	return value, false
}

// Peek reads an element from the ring buffer without pulling (removing) it,
// blocking if the buffer is empty. This allows you to see what the next pull
// will yield without affecting the buffer contents.
func (rb *SyncBuffer[V]) Peek() V {
	rb.pullMu.Lock()

	for {
		if rb.data[rb.read].can.Load() == canReadElement {
			value := rb.data[rb.read].value
			rb.pullMu.Unlock()
			return value
		}

		// Buffer is empty, spin and retry. See [SyncBuffer.Push] for details
		// on how we could avoid spinning in the future.
	}
}

// Pull removes an element from the ring buffer and returns it.
// Pull blocks if the buffer is empty until a new element is pushed to it.
// For more control over pulling, see [SyncBuffer.Poll], [SyncBuffer.Peek],
// and [SyncBuffer.PullWithContext].
func (rb *SyncBuffer[V]) Pull() V {
	rb.pullMu.Lock()

	newRead := rb.read + 1
	if newRead == len(rb.data) {
		newRead = 0
	}

	for {
		if rb.data[rb.read].can.Load() == canReadElement {
			value := rb.data[rb.read].value
			rb.data[rb.read].can.Store(canWriteElement)
			rb.read = newRead

			rb.pullMu.Unlock()
			rb.count.Add(^uintptr(0))
			return value
		}

		// Buffer is empty, spin and retry. See [SyncBuffer.Push] for details
		// on how we could avoid spinning in the future.
	}
}

// Write inserts len(p) elements from p into the ring buffer. Each insertion
// can block according to [SyncBuffer.Push]. Write always returns len(p) and
// a nil error.
//
// If the data contents of the ring buffer are bytes, you can use [SyncBuffer]
// as an [io.Writer].
func (rb *SyncBuffer[V]) Write(p []V) (int, error) {
	for _, value := range p {
		rb.Push(value)
	}
	return len(p), nil
}

// Read pulls (removes) len(p) elements from the ring buffer and stores them
// in p. Read can pull [SyncBuffer.Count] elements at the maximum, at which
// point it returns immediately. Read does not block and returns the number of
// elements less than len(p) successfully pulled. Error is always nil, except
// when len(p) > 0 and no elements are available in the buffer, in which case
// Read returns [io.EOF].
//
// If the data contents of the ring buffer are bytes, you can use [SyncBuffer]
// as an [io.Reader].
func (rb *SyncBuffer[V]) Read(p []V) (int, error) {
	n := 0
	for i := range p {
		value, ok := rb.Poll()
		if ok {
			p[i] = value
			n++
			continue
		}

		var err error
		if n == 0 {
			err = io.EOF
		}
		return n, err
	}
	return n, nil
}

var (
	_ io.ReadWriter = &SyncBuffer[byte]{}
	_ Bufferer[int] = &SyncBuffer[int]{}
)
