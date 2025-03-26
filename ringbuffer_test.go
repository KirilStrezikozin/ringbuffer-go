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

package ring_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	ring "github.com/KirilStrezikozin/ringbuffer-go"
)

func TestBuffer_CountAndLen(t *testing.T) {
	tests := []struct {
		name string // Description of this test case.
		n    int
	}{
		{"sizeEmpty", 0},
		{"sizeOne", 1},
		{"sizeTwo", 2},
		{"sizeMany", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rbuff := ring.New[int](tt.n)
			if rbuff.Count() != 0 {
				t.Errorf("Count() = %v, want %v", rbuff.Count(), 0)
			}
			if rbuff.Len() != tt.n {
				t.Errorf("Len() = %v, want %v", rbuff.Len(), tt.n)
			}
		})
	}
}

func TestBuffer_Poll(t *testing.T) {
	// TODO: test Poll result values, do not do this go func craziness and avoid mutexes.
	rbuff := ring.New[int](10)
	const value = 1

	var wg sync.WaitGroup
	var pushed atomic.Bool
	var mu sync.Mutex

	wg.Add(1)
	ch := make(chan error, 1)
	go func() {
		defer wg.Done()
		defer close(ch)
		for {
			mu.Lock()
			if pushed.Load() {
				if v, ok := rbuff.Poll(); !ok {
					ch <- errors.New("Poll() must retrieve after push")
				} else if v != value {
					ch <- fmt.Errorf("Poll() returned %v, want: %v", v, value)
				}
				mu.Unlock()
				return
			}
			if _, ok := rbuff.Poll(); ok {
				ch <- errors.New("Poll() must not retrieve before push")
				mu.Unlock()
				return
			}
			mu.Unlock()
		}
	}()

	time.Sleep(10 * time.Millisecond)

	mu.Lock()
	rbuff.Push(1)
	mu.Unlock()
	pushed.Store(true)
	wg.Wait()

	if err := <-ch; err != nil {
		t.Fatal(err)
	}
}

func TestBuffer_Offer(t *testing.T) {
	n := 10
	rbuff := ring.New[int](n)

	for i := range rbuff.Len() {
		if ok := rbuff.Offer(i); !ok {
			t.Errorf("Offer() was not expected to block")
		}
	}
}

func TestBuffer_PullWithContext(t *testing.T) {
	n := 10
	rbuff := ring.New[int](n)

	parent, parentCancel := context.WithTimeout(context.Background(), time.Second)
	defer parentCancel()

	var value int
	ctx, cancel := rbuff.PullWithContext(parent, &value)
	defer cancel()

	<-ctx.Done()
	if parent.Err() == nil {
		t.Fatalf("Pull should timeout but PullWithContext() succeeded")
	}
}

func TestBuffer_PushWithContext(t *testing.T) {
	n := 1
	rbuff := ring.New[int](n)

	parent, parentCancel := context.WithTimeout(context.Background(), time.Second)
	defer parentCancel()

	var recvValue int
	ctx, cancel := rbuff.PullWithContext(parent, &recvValue)

	sendValue := 2
	rbuff.Push(sendValue)

	<-ctx.Done()
	cancel()
	if parent.Err() != nil {
		t.Fatalf("PullWithContext() was expected not to timeout")
	}

	if recvValue != sendValue {
		t.Fatalf("Expected to receive %v, got: %v", sendValue, recvValue)
	}

	rbuff.Push(sendValue)
	ctx, cancel = rbuff.PushWithContext(parent, sendValue)
	<-ctx.Done()
	cancel()
	if parent.Err() == nil {
		t.Fatalf("Push should timeout but PushWithContext() succeeded")
	}
}

func TestBuffer_Push(t *testing.T) {
	n := 5
	rbuff := ring.New[int](n)

	for i := range n {
		rbuff.Push(i)
	}

	parent, parentCancel := context.WithTimeout(context.Background(), time.Second)
	defer parentCancel()

	sendValue := n
	if ok := rbuff.Offer(sendValue); ok {
		t.Fatalf("Offer() should return false on full buffer")
	}

	ctx, cancel := rbuff.PushWithContext(parent, sendValue)
	<-ctx.Done()
	if parent.Err() == nil {
		t.Fatalf("Push should timeout but PushWithContext() succeeded")
	}
	cancel()

	if v := rbuff.Peek(); v != 0 {
		t.Fatalf("Peek() = %v, want %v", v, 0)
	}

	if rbuff.Count() != rbuff.Len() {
		t.Fatal("Peek() should not remove elements")
	}

	rbuff.ForcePush(sendValue)
	if v := rbuff.Peek(); v != 1 {
		t.Fatalf("Peek() = %v, want %v", v, 1)
	}

	for i := range n - 1 {
		if v, ok := rbuff.Poll(); !ok {
			t.Fatal("Poll() should return true when buffer has data")
		} else if v != i+1 {
			t.Fatalf("Poll() = (%v, true), want: (%v, true)", v, i+1)
		}

		if rbuff.Count() != n-i-1 {
			t.Fatalf("Count() = %v after removal, want %v", rbuff.Count(), n-i-1)
		}
	}
}

func TestBuffer_OfferSize0(t *testing.T) {
	done := make(chan error, 1)

	go func() {
		var err error

		defer func() {
			if err != nil {
				done <- err
				return
			}

			r := recover()
			if err, ok := r.(error); ok {
				done <- err
			} else {
				close(done)
			}
		}()

		rb := ring.New[byte](0)
		if ok := rb.Offer('h'); ok {
			err = errors.New("Offer() = true, must be false on Buffer with size 0")
		}
	}()

	if err := <-done; err != nil {
		t.Fatalf("Offer() panicked on Buffer with size 0: %s", err)
	}
}
