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
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	ring "github.com/KirilStrezikozin/ringbuffer-go"
	ringtest "github.com/KirilStrezikozin/ringbuffer-go/ringtest"
)

func TestNewSyncFrom(t *testing.T) {
	ringtest.TestNewFrom(
		func(data []int) ringtest.Bufferer[int] {
			return ring.NewSyncFrom(data)
		},
		t,
	)
}

func TestSyncBuffer_ZeroValue(t *testing.T) {
	rb := ring.SyncBuffer[int]{}
	ringtest.TestZeroSize[int](&rb, t)
}

func TestNewSync_ZeroSize(t *testing.T) {
	type Msg struct{}

	rb := ring.NewSync[Msg](0)
	ringtest.TestZeroSize[Msg](rb, t)
}

func TestSyncBuffer_Count(t *testing.T) {
	rb := ring.NewSync[int](2)
	ringtest.TestCount[int](rb, t)
}

func TestSyncBuffer_Len(t *testing.T) {
	rb := ring.SyncBuffer[byte]{}
	if size := rb.Len(); size != 0 {
		t.Errorf("Len() = %v, want %v", size, 0)
	}

	tests := []struct {
		name string
		n    int
	}{
		{"sizeEmpty", 0},
		{"sizeOne", 1},
		{"sizeTwo", 2},
		{"sizeMany", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := ring.NewSync[int](tt.n)
			if size := rb.Len(); size != tt.n {
				t.Errorf("Len() = %v, want %v", size, tt.n)
			}
		})
	}
}

func TestSyncBuffer_Full(t *testing.T) {
	tests := []struct {
		name string
		n    int
	}{
		{"sizeOne", 1},
		{"sizeTwo", 2},
		{"sizeMany", 100},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rb := ring.NewSync[int](tt.n)
			ringtest.TestFull(rb, tt.n, t)
		})
	}
}

func TestSyncBuffer_ForcePush(t *testing.T) {
	rb := ring.NewSync[int](5)
	ringtest.TestForcePush(rb, t)
}

func TestSyncBuffer_Offer(t *testing.T) {
	rb := ring.NewSync[int](5)
	ringtest.TestOffer(rb, t)
}

func TestSyncBuffer_PushWithContext(t *testing.T) {
	ringtest.TestPushWithContext(
		func(n int) ringtest.Bufferer[int] {
			return ring.NewSync[int](n)
		},
		t,
	)
}

func TestSyncBuffer_Peek(t *testing.T) {
	ringtest.TestPeek(
		func(data []int) ringtest.Bufferer[int] {
			return ring.NewSyncFrom(data)
		},
		t,
	)
}

func TestSyncBuffer_Poll(t *testing.T) {
	ringtest.TestPoll(
		func(n int) ringtest.Bufferer[int] {
			return ring.NewSync[int](n)
		},
		t,
	)
}

func TestSyncBuffer_PullWithContext(t *testing.T) {
	ringtest.TestPullWithContext(
		func(n int) ringtest.Bufferer[int] {
			return ring.NewSync[int](n)
		},
		t,
	)
}

func TestSyncBuffer_Write(t *testing.T) {
	n := 8
	rb := ring.NewSync[int](n)

	if rb.Len() != n {
		t.Fatalf("Len() = %v, want: %v", rb.Len(), n)
	}

	dataN := 4 * n
	data := make([]int, dataN)
	for i := 0; i < len(data); i++ {
		data[i] = i + 1
	}

	chWrite := make(chan error)
	go func() {
		// Write will block once it writes n elements.
		// The other go-routine below will sense this and pull n elements out,
		// after which this Write will continue its work, until all dataN
		// elements have been written, at which point the two go-routines
		// should exit.

		if n, err := rb.Write(data); err != nil {
			chWrite <- fmt.Errorf("Write(): expected nil err, got: %v", err)
			return
		} else if n != len(data) {
			chWrite <- fmt.Errorf("Write(): expected to write %d elements, but wrote %d", len(data), n)
			return
		}
		close(chWrite)
	}()

	chRead := make(chan error)
	go func() {
		written := 0
		for {
			if !rb.Full() {
				continue
			}

			for i := 0; i < rb.Len(); i++ {
				if _, ok := rb.Poll(); !ok {
					chRead <- fmt.Errorf("Poll(): failed to pull data[%d]", written+i)
					return
				}
				written++
			}

			if written == dataN {
				close(chRead)
				return
			}
		}
	}()

	// Test fails on error without waiting for other go-routines to finish.
	// This prevents us from waiting indefinitely for this test to finish.
	select {
	case err := <-chWrite:
		if err != nil {
			t.Fatal(err)
		}
		<-chRead
	case err := <-chRead:
		if err != nil {
			t.Fatal(err)
		}
		<-chWrite
	}

	if count := rb.Count(); count != 0 {
		t.Fatalf("Count() = %v, want: %v", count, 0)
	}
}

func TestSyncBuffer_Read(t *testing.T) {
	ringtest.TestRead(
		func(n int) ringtest.Bufferer[int] {
			return ring.NewSync[int](n)
		},
		t,
	)
}

func TestSyncBuffer_DataRace(t *testing.T) {
	// Running Go race detector successfully and correctly detects any data
	// races. This test exists as a nuclear way to spot invalid or messed up
	// values returned by Pull()s.

	type Payload struct {
		ID   int
		Text string
	}

	n := 100000
	rb := ring.NewSync[Payload](1)

	var count uintptr
	var overlaps uintptr

	var wg sync.WaitGroup
	var m sync.Map

	// Spawn concurrent ring buffer consumers (readers).
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			p := rb.Pull()
			if _, loaded := m.LoadOrStore(p.ID, true); loaded {
				// Element with this ID has been already pulled,
				// this must be a data race.
				atomic.AddUintptr(&overlaps, 1)
			}

			atomic.AddUintptr(&count, 1)
			wg.Done()
		}()
	}

	// Spawn concurrent ring buffer producers (writers).
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(k int) {
			p := Payload{
				ID:   k,
				Text: fmt.Sprintf("hello from %d", k),
			}
			rb.Push(p)
			wg.Done()
		}(i)
	}

	wg.Wait()

	if c := atomic.LoadUintptr(&count); c != uintptr(n) {
		t.Fatalf("Pulled %v elements instead of %v", c, n)
	}

	if o := atomic.LoadUintptr(&overlaps); o != 0 {
		t.Fatalf("%d broken elements, must be 0", o)
	}
}
