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

// Package ringtest provides common testing utilities for the [ring] package.
package ringtest

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	ring "github.com/KirilStrezikozin/ringbuffer-go"
)

// Bufferer wraps all methods of [ring.Buffer] and [ring.SyncBuffer] for
// test helpers that check results of calling common methods.
type Bufferer[V any] interface {
	ring.Bufferer[V]

	Len() int
	Full() bool

	ForcePush(value V)
	Offer(value V) bool
	PushWithContext(parent context.Context, value V) (context.Context, context.CancelFunc)

	Peek() V
	Poll() (V, bool)
	PullWithContext(parent context.Context, valuePtr *V) (context.Context, context.CancelFunc)

	Write(p []V) (n int, err error)
	Read(p []V) (n int, err error)
}

// TakePanic is a helper function that recovers a
// panic from fun and returns it as error.
func TakePanic(fun func()) (err error) {
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer func() {
			r := recover()
			if recoverErr, ok := r.(error); ok {
				err = recoverErr
			} else if s, ok := r.(string); ok {
				err = errors.New(s)
			} else if stringer, ok := r.(interface{ String() string }); ok {
				err = errors.New(stringer.String())
			} else {
				panic("unknown recover value type")
			}
			wg.Done()
		}()

		fun()
		wg.Done()
	}()

	wg.Wait()
	return err
}

// TestNewFrom is a test helper to test calls to [ring.NewFrom] and [ring.NewSyncFrom].
func TestNewFrom(newBuffFrom func([]int) Bufferer[int], t *testing.T) {
	data := make([]int, 3, 4)
	data[0] = 1
	data[1] = 2
	data[2] = 3

	rb := newBuffFrom(data)

	if rb.Len() != cap(data) {
		t.Fatalf("Len() = %v, want: %v", rb.Len(), cap(data))
	}

	if count := rb.Count(); count != len(data) {
		t.Fatalf("Count() = %v, want: %v", count, len(data))
	}

	data = append(data, 4)

	if !rb.Offer(data[3]) {
		t.Fatal("Offer(): must be true on not full buffer")
	}

	if rb.Offer(5) {
		t.Fatal("Offer(): must return false on full buffer")
	}

	data[0] = 5
	if rb.Pull() != 1 {
		t.Fatal("Must not retain the given data")
	}
}

// TestZeroSize is a test helper to test operations on a zero-sized ring buffer.
func TestZeroSize[V any](rb Bufferer[V], t *testing.T) {
	if count := rb.Count(); count != 0 {
		t.Fatalf("Count(): want: %v, got: %v", 0, count)
	}

	if size := rb.Len(); size != 0 {
		t.Fatalf("Count(): want: %v, got: %v", 0, size)
	}

	if !rb.Full() {
		t.Fatal("Full(): must be true on zero-sized buffer")
	}

	var value V // A dummy value.

	if ok := rb.Offer(value); ok {
		t.Fatal("Offer(): must not succeed on zero-sized buffer")
	}

	if _, ok := rb.Poll(); ok {
		t.Fatal("Poll(): must not succeed on zero-sized buffer")
	}

	if err := TakePanic(func() {
		_ = rb.Pull()
	}); err == nil {
		t.Fatal("Pull(): must panic on zero-sized buffer")
	}

	if err := TakePanic(func() {
		_ = rb.Peek()
	}); err == nil {
		t.Fatal("Peek(): must panic on zero-sized buffer")
	}

	if err := TakePanic(func() {
		rb.Push(value)
	}); err == nil {
		t.Fatal("Push(): must panic on zero-sized buffer")
	}

	if err := TakePanic(func() {
		rb.ForcePush(value)
	}); err == nil {
		t.Fatal("Push(): must panic on zero-sized buffer")
	}

	timeout := 100 * time.Millisecond

	parentPush, parentPushCancel := context.WithTimeout(context.Background(), timeout)
	defer parentPushCancel()

	ctx, cancel := rb.PushWithContext(parentPush, value)
	<-ctx.Done()
	cancel()
	if parentPush.Err() == nil {
		t.Fatal("PushWithContext(): must timeout on zero-sized buffer")
	}

	parentPull, parentPullCancel := context.WithTimeout(context.Background(), timeout)
	defer parentPullCancel()

	ctx, cancel = rb.PullWithContext(parentPull, &value)
	defer cancel()

	<-ctx.Done()
	if parentPull.Err() == nil {
		t.Fatal("PullWithContext(): must timeout on zero-sized buffer")
	}
}

// TestCount is a test helper to test Count operations on a ring buffer.
func TestCount[V any](rb Bufferer[V], t *testing.T) {
	n := rb.Len()
	var value V // A dummy value.

	if count := rb.Count(); count != 0 {
		t.Fatalf("Count() = %v, want: %v", count, 0)
	}

	rb.Push(value)

	if count := rb.Count(); count != 1 {
		t.Fatalf("Count() = %v, want: %v", count, 1)
	}

	rb.Push(value)

	if count := rb.Count(); count != 2 {
		t.Fatalf("Count() = %v, want: %v", count, 2)
	}

	_ = rb.Pull()
	rb.Push(value)

	if count := rb.Count(); count != 2 {
		t.Fatalf("Count() = %v, want: %v", count, 2)
	}

	_ = rb.Pull()

	if count := rb.Count(); count != 1 {
		t.Fatalf("Count() = %v, want: %v", count, 1)
	}

	for range 2*n + 1 {
		rb.ForcePush(value)
	}

	if count := rb.Count(); count != rb.Len() {
		t.Fatalf("Count() = %v, want: %v", count, rb.Len())
	}

	for i := range n {
		if count := rb.Count(); count != n-i {
			t.Fatalf("Count() = %v, want: %v", count, n-i)
		}
		_ = rb.Pull()
	}
}

// TestFull is a test helper to test Full calls on a ring buffer.
func TestFull(rb Bufferer[int], n int, t *testing.T) {
	for i := range n {
		if i < n-1 && rb.Full() {
			t.Error("Full(): must be false on not full buffer")
		}
		rb.Push(i)
	}

	if !rb.Full() {
		t.Error("Full(): must be true on full buffer")
	}

	_ = rb.Pull()
	rb.Push(1)

	if !rb.Full() {
		t.Error("Full(): must be true on full buffer")
	}
}

// TestForcePush is a test helper to test ForcePush calls on a ring buffer.
func TestForcePush(rb Bufferer[int], t *testing.T) {
	n := rb.Len()

	for i := range n {
		rb.ForcePush(i + 1)
		if count := rb.Count(); count != i+1 {
			t.Fatalf("Count() = %v, want: %v", count, i+1)
		}
	}

	if rb.Offer(n + 1) {
		t.Fatal("Offer(): must return false on full buffer")
	}

	if value := rb.Pull(); value != 1 {
		t.Fatalf("Pull(): expected last value: %v, got: %v", n, value)
	}

	rb.ForcePush(n + 1)
	if value := rb.Pull(); value != 2 {
		t.Fatalf("Pull(): expected last value: %v, got: %v", n+1, value)
	}

	rb.ForcePush(n + 1)
	if rb.Offer(n + 1) {
		t.Fatal("Offer(): must return false on full buffer")
	}
}

// TestOffer is a test helper to test Offer calls on a ring buffer.
func TestOffer(rb Bufferer[int], t *testing.T) {
	n := rb.Len()

	for i := range n {
		if ok := rb.Offer(i + 1); !ok {
			t.Fatal("Offer(): must be true on not full buffer")
		}
	}

	if rb.Offer(n + 1) {
		t.Fatal("Offer(): must return false on full buffer")
	}

	if value := rb.Pull(); value != 1 {
		t.Fatalf("Pull(): expected last value: %v, got: %v", n, value)
	}

	if !rb.Offer(n + 1) {
		t.Fatal("Offer(): must be true on not full buffer")
	}

	if value := rb.Pull(); value != 2 {
		t.Fatalf("Pull(): expected last value: %v, got: %v", n+1, value)
	}
}

// TestPushWithContext is a test helper to test PushWithContext calls on a ring buffer.
func TestPushWithContext(newBuff func(int) Bufferer[int], t *testing.T) {
	rb := newBuff(1)
	if rb.Len() != 1 {
		panic("need ring buffer with length 1")
	}

	timeout := 100 * time.Millisecond

	parent1, parent1Cancel := context.WithTimeout(context.Background(), timeout)
	defer parent1Cancel()

	sendValue := 1
	rb.Push(sendValue)

	ctx, cancel := rb.PushWithContext(parent1, sendValue)
	<-ctx.Done()
	cancel()
	if parent1.Err() == nil {
		t.Fatal("Push should timeout but PushWithContext() succeeded")
	}

	if value := rb.Pull(); value != sendValue {
		t.Fatalf("Pull() = %v, want: %v", value, sendValue)
	}

	parent2, parent2Cancel := context.WithTimeout(context.Background(), timeout)
	defer parent2Cancel()

	ctx, cancel = rb.PushWithContext(parent2, sendValue)
	<-ctx.Done()
	cancel()
	if parent2.Err() != nil {
		t.Fatal("Push should succeed but PushWithContext() timed-out")
	}

	ctx, cancel = rb.PushWithContext(parent1, sendValue)
	<-ctx.Done()
	cancel()
	if parent1.Err() == nil {
		t.Fatal("Push should cancel but PushWithContext() succeeded")
	}
}

// TestPeek is a test helper to test Peek calls on a ring buffer.
func TestPeek(newBuffFrom func([]int) Bufferer[int], t *testing.T) {
	data := []int{1, 2, 3}
	rb := newBuffFrom(data)

	if !rb.Full() {
		t.Fatal("Full(): must return true on full buffer")
	}

	if rb.Offer(4) {
		t.Fatal("Offer(): must return false on full buffer")
	}

	if value := rb.Peek(); value != data[0] {
		t.Fatalf("Peek() = %v, want: %v", value, data[0])
	}

	if !rb.Full() {
		t.Fatal("Full(): must return true on full buffer")
	}

	_ = rb.Pull()

	if value := rb.Peek(); value != data[1] {
		t.Fatalf("Peek() = %v, want: %v", value, data[1])
	}

	if count := rb.Count(); count != len(data)-1 {
		t.Fatalf("Count() = %v, want: %v", count, len(data)-1)
	}
}

// TestPoll is a test helper to test Poll calls on a ring buffer.
func TestPoll(newBuff func(int) Bufferer[int], t *testing.T) {
	rb := newBuff(2)

	if _, ok := rb.Poll(); ok {
		t.Fatal("Poll(): must return false on empty buffer")
	}

	rb.Push(1)
	rb.Push(2)

	if value, ok := rb.Poll(); !ok {
		t.Fatal("Poll(): must return true on not empty buffer")
	} else if value != 1 {
		t.Fatalf("Poll(): returned valued %d, want: %d", value, 1)
	}

	if value, ok := rb.Poll(); !ok {
		t.Fatal("Poll(): must return true on not empty buffer")
	} else if value != 2 {
		t.Fatalf("Poll(): returned valued %d, want: %d", value, 2)
	}

	if _, ok := rb.Poll(); ok {
		t.Fatal("Poll(): must return false on empty buffer")
	}
}

// TestPullWithContext is a test helper to test PullWithContext calls on a ring buffer.
func TestPullWithContext(newBuff func(int) Bufferer[int], t *testing.T) {
	rb := newBuff(1)
	if rb.Len() != 1 {
		panic("need ring buffer with length 1")
	}

	var value int
	timeout := 100 * time.Millisecond

	parent1, parent1Cancel := context.WithTimeout(context.Background(), timeout)
	defer parent1Cancel()

	ctx, cancel := rb.PullWithContext(parent1, &value)
	defer cancel()

	<-ctx.Done()
	if parent1.Err() == nil {
		t.Fatal("Pull should timeout but PullWithContext() succeeded")
	}

	sendValue := 1
	rb.Push(sendValue)

	parent2, parent2Cancel := context.WithTimeout(context.Background(), timeout)
	defer parent2Cancel()

	ctx, cancel = rb.PullWithContext(parent2, &value)
	defer cancel()

	<-ctx.Done()
	if parent2.Err() != nil {
		t.Fatal("Pull should succeed but PullWithContext() timed-out")
	}
}

// TestRead is a test helper to test Read calls on a ring buffer.
func TestRead(newBuff func(int) Bufferer[int], t *testing.T) {
	n := 32
	rb := newBuff(n)

	if rb.Len() != n {
		t.Fatalf("Len() = %v, want: %v", rb.Len(), n)
	}

	nData := 10
	for i := range nData {
		rb.Push(i + 1)
	}

	if count := rb.Count(); count != nData {
		t.Fatalf("Count() = %v, want: %v", count, nData)
	}

	data := make([]int, n)
	if n, err := rb.Read(data); err != nil {
		t.Fatalf("Read(): expected nil err, got: %v", err)
	} else if n != nData {
		t.Fatalf("Read(): expected to read %d elements, but read %d", nData, n)
	}

	if count := rb.Count(); count != 0 {
		t.Fatalf("Count() = %v, want: %v", count, 0)
	}

	if n, err := rb.Read(data); err != io.EOF {
		t.Fatalf("Read(): expected err: %v, got: %v", io.EOF, err)
	} else if n != 0 {
		t.Fatalf("Read(): expected to read %d elements, but read %d", 0, n)
	}
}
