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
)

// Bufferer is the interface of a circular (ring) buffer that
// wraps the Count, Push, and Pull operations on the buffer.
type Bufferer[V any] interface {
	// Count returns the number of elements currently stored in the ring buffer.
	Count() int

	// Push inserts a new element into the ring buffer, blocking if the buffer
	// is full. It blocks indefinitely if the buffer is full and no reader ever
	// reads an element from the buffer.
	Push(value V)

	// Pull removes an element from the ring buffer and returns it.
	// Pull blocks if the buffer is empty until a new element is pushed to it.
	Pull() V
}

// Buffer implements [Bufferer], a circular (ring) fixed-size buffer with
// constant time O(1) element adding and removing operations. Use [New] to
// create a ring buffer.
//
// The zero value is a buffer of size zero, on which blocking push and pull
// operations panic.
//
// Buffer with element type `byte` implements [io.ReadWriter] interface.
type Buffer[V any] struct {
	count int
	write int
	read  int

	data []V
}

// New allocates and returns a new fixed-size circular (ring) [Buffer] that
// fits n elements in total.
func New[V any](n int) *Buffer[V] {
	return &Buffer[V]{
		data: make([]V, n),
	}
}

// NewFrom allocates and returns a new circular (ring) buffer with contents
// pre-populated from data. Data is assumed to already contain elements,
// as such [Buffer] will store the next element to push at len(data),
// and wrap to the beginning when cap(data) is reached. Pulling will
// start from the beginning.
//
// The returned [Buffer] does not retain data.
//
// See also: [New].
func NewFrom[V any](data []V) *Buffer[V] {
	write := len(data)
	if write == cap(data) {
		write = 0
	}

	rb := &Buffer[V]{
		write: write,
		count: len(data),
		data:  make([]V, cap(data)),
	}

	copy(rb.data, data)
	return rb
}

// Count returns the number of elements currently stored in the ring buffer.
func (rb *Buffer[V]) Count() int {
	return rb.count
}

// Len returns the size of the ring buffer's data. This value is the maximum
// number of elements the buffer can hold.
func (rb *Buffer[V]) Len() int {
	return len(rb.data)
}

// Full reports whether the ring buffer is full.
func (rb *Buffer[V]) Full() bool {
	return rb.count == len(rb.data)
}

// ForcePush performs a non-blocking value insertion into the ring buffer.
// If the buffer is full, it overwrites the last element and advances the
// reading position.
func (rb *Buffer[V]) ForcePush(value V) {
	if len(rb.data) == 0 { // Fast path.
		// Force-push to zero-sized buffer.
		panic("force-push to zero-sized buffer")
	}

	// Slow path.
	if rb.write == rb.read && rb.count != 0 {
		newRead := rb.read + 1
		if newRead == len(rb.data) {
			newRead = 0
		}
		rb.read = newRead
	} else {
		rb.count++
	}

	newWrite := rb.write + 1
	if newWrite == len(rb.data) {
		newWrite = 0
	}

	rb.data[rb.write] = value
	rb.write = newWrite
}

// Offer performs a non-blocking value insertion into the ring buffer.
// It returns true if the buffer is not full and insertion succeeds. Otherwise,
// it immediately returns false.
func (rb *Buffer[V]) Offer(value V) bool {
	if len(rb.data) == 0 { // Fast path.
		return false
	}

	// Slow path.
	if rb.write == rb.read && rb.count != 0 {
		return false
	}

	newWrite := rb.write + 1
	if newWrite == len(rb.data) {
		newWrite = 0
	}

	rb.data[rb.write] = value
	rb.write = newWrite
	rb.count++
	return true
}

// Push inserts a new element into the ring buffer, blocking if the buffer is
// full. It blocks indefinitely if the buffer is full and no reader ever
// reads an element from the buffer. For more control over insertion,
// see [Buffer.Offer] and [Buffer.PushWithContext].
func (rb *Buffer[V]) Push(value V) {
	if len(rb.data) == 0 { // Fast path.
		// Push to zero-sized buffer, panic instead of blocking forever.
		panic("push to zero-sized buffer")
	}

	// Slow path.
	for {
		if rb.write == rb.read && rb.count != 0 {
			continue
		}

		newWrite := rb.write + 1
		if newWrite == len(rb.data) {
			newWrite = 0
		}

		rb.data[rb.write] = value
		rb.write = newWrite
		rb.count++
		break
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
func (rb *Buffer[V]) PushWithContext(parent context.Context, value V) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	if ctx.Err() != nil {
		return ctx, cancel
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				if ok := rb.Offer(value); ok {
					cancel()
					return
				}

				// Buffer is full, spin and retry. It would be much better to
				// wake this go-routine when push can proceed or when parent
				// context cancels. See [SyncBuffer.Push] for some details.
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
func (rb *Buffer[V]) Poll() (V, bool) {
	var value V

	if len(rb.data) == 0 { // Fast path.
		return value, false
	}

	// Slow path.
	if rb.count == 0 {
		return value, false
	}

	newRead := rb.read + 1
	if newRead == len(rb.data) {
		newRead = 0
	}

	value = rb.data[rb.read]
	rb.read = newRead
	rb.count--
	return value, true
}

// Peek reads an element from the ring buffer without pulling (removing) it,
// blocking if the buffer is empty. This allows you to see what the next pull
// will yield without affecting the buffer contents.
func (rb *Buffer[V]) Peek() V {
	if len(rb.data) == 0 { // Fast path.
		// Peek into zero-sized buffer, panic instead of blocking forever.
		panic("peek into zero-sized buffer")
	}

	// Slow path.
	for {
		if rb.count != 0 {
			return rb.data[rb.read]
		}
	}
}

// Pull removes an element from the ring buffer and returns it.
// Pull blocks if the buffer is empty until a new element is pushed to it.
// For more control over pulling, see [Buffer.Poll], [Buffer.Peek],
// and [Buffer.PullWithContext].
func (rb *Buffer[V]) Pull() V {
	if len(rb.data) == 0 { // Fast path.
		// Pull from zero-sized buffer, panic instead of blocking forever.
		panic("pull from zero-sized buffer")
	}

	// Slow path.
	for {
		if rb.count == 0 {
			continue
		}

		newRead := rb.read + 1
		if newRead == len(rb.data) {
			newRead = 0
		}

		value := rb.data[rb.read]
		rb.read = newRead
		rb.count--
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
func (rb *Buffer[V]) PullWithContext(parent context.Context, valuePtr *V) (context.Context, context.CancelFunc) {
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	if ctx.Err() != nil {
		return ctx, cancel
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				// Avoid polling unnecessarily.
				if rb.count == 0 {
					continue
				}

				if value, ok := rb.Poll(); ok {
					*valuePtr = value
					cancel()
					return
				}

				// Buffer is empty, spin and retry. It would be much better to
				// wake this go-routine when pull can proceed or when parent
				// context cancels. See [SyncBuffer.Push] for some details.
			}
		}
	}()
	return ctx, cancel
}

// Write inserts len(p) elements from p into the ring buffer. Each insertion
// can block according to [Buffer.Push]. Write always returns len(p) and
// a nil error.
//
// If the data contents of the ring buffer are bytes, you can use [Buffer]
// as an [io.Writer].
func (rb *Buffer[V]) Write(p []V) (int, error) {
	for _, value := range p {
		rb.Push(value)
	}
	return len(p), nil
}

// Read pulls (removes) len(p) elements from the ring buffer and stores them
// in p. Read can pull Buffer.Count() elements at the maximum, at which
// point it returns immediately. Read does not block and returns the number of
// elements less than len(p) successfully pulled. Error is always nil, except
// when len(p) > 0 and no elements are available in the buffer, in which case
// Read returns [io.EOF].
//
// If the data contents of the ring buffer are bytes, you can use [Buffer]
// as an [io.Reader].
func (rb *Buffer[V]) Read(p []V) (int, error) {
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
	_ io.ReadWriter = &Buffer[byte]{}
	_ Bufferer[int] = &Buffer[int]{}
)
