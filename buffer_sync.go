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
	"context"
	"io"
	"runtime"
	"sync/atomic"
)

// Determines the right to read/write the buffer's element.
const (
	canWriteElement = uint32(iota)
	canIsWritingElement
	canReadElement
	canIsReadingElement
)

// Element represents an item in [SyncBuffer].
type element[V any] struct {
	can   uint32
	value V
}

// SyncBuffer implements [Bufferer], a circular (ring) fixed-size buffer with
// constant time O(1) element adding and removing operations. SyncBuffer is
// safe for concurrent use. Use [NewSync] to create it.
//
// The zero value is a buffer of size zero, on which blocking push and pull
// operations panic.
//
// SyncBuffer with element type `byte` implements [io.ReadWriter] interface.
type SyncBuffer[V any] struct {
	_ noCopy

	write uintptr
	read  uintptr

	data []element[V]
}

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
	write := len(data)
	if write == cap(data) {
		write = 0
	}

	rb := &SyncBuffer[V]{
		write: uintptr(write),
		data:  make([]element[V], cap(data)),
	}

	for i, v := range data {
		rb.data[i].value = v
		rb.data[i].can = canReadElement
	}

	return rb
}

// Count returns the number of elements currently stored in the ring buffer.
func (rb *SyncBuffer[V]) Count() int {
	if len(rb.data) == 0 { // Fast path.
		return 0
	}

	// Slow path. The actual number of elements the buffer holds may have
	// changed by the time the calling function receives the return value.
	write := atomic.LoadUintptr(&rb.write)
	read := atomic.LoadUintptr(&rb.read)

	if write > read {
		return int(write - read)
	} else if write < read {
		return int(uintptr(len(rb.data)) - read + write)
	}

	switch atomic.LoadUint32(&rb.data[write].can) {
	case canWriteElement, canIsWritingElement:
		return 0
	default:
		return int(uintptr(len(rb.data)) - read + write)
	}
}

// Len returns the size of the ring buffer's data. This value is the maximum
// number of elements the buffer can hold.
func (rb *SyncBuffer[V]) Len() int {
	// rb.data is never re-sliced, thus this value never changes.
	return len(rb.data)
}

// Full reports whether the ring buffer is full.
func (rb *SyncBuffer[V]) Full() bool {
	return rb.Count() == len(rb.data)
}

// ForcePush performs a non-blocking value insertion into the ring buffer.
// If the buffer is full, it overwrites the last element and advances the
// reading position.
func (rb *SyncBuffer[V]) ForcePush(value V) {
	if len(rb.data) == 0 { // Fast path.
		// Force-push to zero-sized buffer.
		panic("force-push to zero-sized buffer")
	}

	// Slow path.
	for {
		write := atomic.LoadUintptr(&rb.write)
		newWrite := write + 1
		if newWrite == uintptr(len(rb.data)) {
			newWrite = 0
		}

		e := &rb.data[write]
		if atomic.CompareAndSwapUint32(&e.can, canReadElement, canIsWritingElement) {
			read := atomic.LoadUintptr(&rb.read)
			newRead := read + 1
			if newRead == uintptr(len(rb.data)) {
				newRead = 0
			}
			atomic.StoreUintptr(&rb.read, newRead)
		} else if !atomic.CompareAndSwapUint32(&e.can, canWriteElement, canIsWritingElement) {
			// Someone else is either concurrently writing the same element
			// or reading it. Allow other go-routines to run and retry.
			runtime.Gosched()
			continue
		}

		atomic.StoreUintptr(&rb.write, newWrite)
		e.value = value
		atomic.StoreUint32(&e.can, canReadElement)
		return
	}
}

// Offer performs a non-blocking value insertion into the ring buffer.
// It returns true if the buffer is not full and insertion succeeds. Otherwise,
// it immediately returns false.
func (rb *SyncBuffer[V]) Offer(value V) bool {
	if len(rb.data) == 0 { // Fast path.
		return false
	}

	// Slow path.
	for {
		write := atomic.LoadUintptr(&rb.write)
		newWrite := write + 1
		if newWrite == uintptr(len(rb.data)) {
			newWrite = 0
		}

		e := &rb.data[write]
		if atomic.LoadUint32(&e.can) >= canReadElement {
			// We bumped into the element that is awaiting to be read. Buffer is full.
			return false
		}

		if !atomic.CompareAndSwapUint32(&e.can, canWriteElement, canIsWritingElement) {
			// Someone else has concurrently locked the element for writing.
			// Allow other go-routines to run and retry.
			runtime.Gosched()
			continue
		}

		atomic.StoreUintptr(&rb.write, newWrite)
		e.value = value
		atomic.StoreUint32(&e.can, canReadElement)
		return true
	}
}

// Push inserts a new element into the ring buffer, blocking if the buffer is
// full. It blocks indefinitely if the buffer is full and no reader ever
// reads an element from the buffer. For more control over insertion,
// see [SyncBuffer.Offer] and [SyncBuffer.PushWithContext].
func (rb *SyncBuffer[V]) Push(value V) {
	if len(rb.data) == 0 { // Fast path.
		// Push to zero-sized buffer, panic instead of blocking forever.
		panic("push to zero-sized buffer")
	}

	// Slow path.
	for {
		write := atomic.LoadUintptr(&rb.write)
		newWrite := write + 1
		if newWrite == uintptr(len(rb.data)) {
			newWrite = 0
		}

		e := &rb.data[write]
		if !atomic.CompareAndSwapUint32(&e.can, canWriteElement, canIsWritingElement) {
			// Someone else has concurrently locked the element for
			// writing or the element is awaiting to be read. Allow
			// other go-routines to run and retry.
			runtime.Gosched()
			continue
		}

		atomic.StoreUintptr(&rb.write, newWrite)
		e.value = value
		atomic.StoreUint32(&e.can, canReadElement)
		return
	}
}

// PushWithContext returns a copy of the parent context that is marked done
// (its Done channel is closed) when the value is inserted into the ring
// buffer, when the returned cancel function is called, or when the parent
// context's Done channel is closed, whichever happens first.
//
// If the provided parent context is nil, [context.Background] will be used.
//
// The cancel function releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete or
// the value no longer needs to be inserted.
func (rb *SyncBuffer[V]) PushWithContext(parent context.Context, value V) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
	if ctx.Err() != nil || len(rb.data) == 0 {
		// Fast path. Either parent context is cancelled or buffer is
		// zero-sized. Let the caller wait for parent context to cancel.
		return ctx, cancel
	}

	// Slow path. It would be much better to avoid active spinning, and
	// wake this go-routine up when push can proceed or parent context cancels.
	// See https://github.com/golang/go/issues/8899#issuecomment-204886156.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				write := atomic.LoadUintptr(&rb.write)
				newWrite := write + 1
				if newWrite == uintptr(len(rb.data)) {
					newWrite = 0
				}

				e := &rb.data[write]
				if !atomic.CompareAndSwapUint32(&e.can, canWriteElement, canIsWritingElement) {
					// Someone else has concurrently locked the element for
					// writing or the element is awaiting to be read. Allow
					// other go-routines to run and retry.
					runtime.Gosched()
					continue
				}

				atomic.StoreUintptr(&rb.write, newWrite)
				e.value = value
				atomic.StoreUint32(&e.can, canReadElement)

				cancel() // Let the caller know we succeeded.
				return
			}
		}
	}()

	return ctx, cancel
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

	if len(rb.data) == 0 { // Fast path.
		return value, false
	}

	// Slow path.
	for {
		read := atomic.LoadUintptr(&rb.read)
		newRead := read + 1
		if newRead == uintptr(len(rb.data)) {
			newRead = 0
		}

		e := &rb.data[read]
		if atomic.LoadUint32(&e.can) < canReadElement {
			// We bumped into the element that is awaiting to be written. Buffer is empty.
			return value, false
		}

		if !atomic.CompareAndSwapUint32(&e.can, canReadElement, canIsReadingElement) {
			// Someone else has concurrently locked the element for reading.
			// Allow other go-routines to run and retry.
			runtime.Gosched()
			continue
		}

		atomic.StoreUintptr(&rb.read, newRead)
		value = e.value
		atomic.StoreUint32(&e.can, canWriteElement)
		return value, true
	}
}

// Peek reads an element from the ring buffer without pulling (removing) it,
// blocking if the buffer is empty. This allows you to see what the next pull
// will yield without affecting the buffer contents.
func (rb *SyncBuffer[V]) Peek() V {
	if len(rb.data) == 0 { // Fast path.
		// Peek into zero-sized buffer, panic instead of blocking forever.
		panic("peek into zero-sized buffer")
	}

	// Slow path.
	for {
		read := atomic.LoadUintptr(&rb.read)
		e := &rb.data[read]
		if !atomic.CompareAndSwapUint32(&e.can, canReadElement, canIsReadingElement) {
			// Someone else has concurrently locked the element for
			// reading or the element is awaiting to be written. Allow
			// other go-routines to run and retry.
			runtime.Gosched()
			continue
		}

		value := e.value
		atomic.StoreUint32(&e.can, canReadElement)
		return value
	}
}

// Pull removes an element from the ring buffer and returns it.
// Pull blocks if the buffer is empty until a new element is pushed to it.
// For more control over pulling, see [SyncBuffer.Poll], [SyncBuffer.Peek],
// and [SyncBuffer.PullWithContext].
func (rb *SyncBuffer[V]) Pull() V {
	if len(rb.data) == 0 { // Fast path.
		// Pull from zero-sized buffer, panic instead of blocking forever.
		panic("pull from zero-sized buffer")
	}

	// Slow path.
	for {
		read := atomic.LoadUintptr(&rb.read)
		newRead := read + 1
		if newRead == uintptr(len(rb.data)) {
			newRead = 0
		}

		e := &rb.data[read]
		if !atomic.CompareAndSwapUint32(&e.can, canReadElement, canIsReadingElement) {
			// Someone else has concurrently locked the element for
			// reading or the element is awaiting to be written. Allow
			// other go-routines to run and retry.
			runtime.Gosched()
			continue
		}

		atomic.StoreUintptr(&rb.read, newRead)
		value := e.value
		atomic.StoreUint32(&e.can, canWriteElement)
		return value
	}
}

// PullWithContext returns a copy of the parent context that is marked done
// (its Done channel is closed) when an element is pulled from the ring
// buffer, when the returned cancel function is called, or when the parent
// context's Done channel is closed, whichever happens first.
//
// If the provided parent context is nil, [context.Background] will be used.
//
// When an element is successfully pulled (removed) from the ring buffer,
// it is stored in the given value. Clients are allowed to use the element
// stored in the value after the pull completes.
//
// The cancel function releases resources associated with it, so code should
// call cancel as soon as the operations running in this Context complete or
// the value no longer needs to be pulled from the ring buffer.
func (rb *SyncBuffer[V]) PullWithContext(parent context.Context, valuePtr *V) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}

	ctx, cancel := context.WithCancel(parent)
	if ctx.Err() != nil || len(rb.data) == 0 {
		// Fast path. Either parent context is cancelled or buffer is
		// zero-sized. Let the caller wait for parent context to cancel.
		return ctx, cancel
	}

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				read := atomic.LoadUintptr(&rb.read)
				newRead := read + 1
				if newRead == uintptr(len(rb.data)) {
					newRead = 0
				}

				e := &rb.data[read]
				if !atomic.CompareAndSwapUint32(&e.can, canReadElement, canIsReadingElement) {
					// Someone else has concurrently locked the element for
					// reading or the element is awaiting to be written. Allow
					// other go-routines to run and retry.
					runtime.Gosched()
					continue
				}

				atomic.StoreUintptr(&rb.read, newRead)
				*valuePtr = e.value
				atomic.StoreUint32(&e.can, canWriteElement)

				cancel() // Let the caller know we succeeded.
				return
			}
		}
	}()

	return ctx, cancel
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
// in p. Read can pull SyncBuffer.Count() elements at the maximum, at which
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
