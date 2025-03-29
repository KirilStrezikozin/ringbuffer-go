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
	"testing"

	ring "github.com/KirilStrezikozin/ringbuffer-go"
	ringtest "github.com/KirilStrezikozin/ringbuffer-go/ringtest"
)

func TestNewFrom(t *testing.T) {
	ringtest.TestNewFrom(
		func(data []int) ringtest.Bufferer[int] {
			return ring.NewFrom(data)
		},
		t,
	)
}

func TestBuffer_ZeroValue(t *testing.T) {
	rb := ring.Buffer[int]{}
	ringtest.TestZeroSize(&rb, t)
}

func TestNew_ZeroSize(t *testing.T) {
	type Msg struct{}

	rb := ring.New[Msg](0)
	ringtest.TestZeroSize(rb, t)
}

func TestBuffer_Count(t *testing.T) {
	rb := ring.New[int](2)
	ringtest.TestCount(rb, t)
}

func TestBuffer_Len(t *testing.T) {
	rb := ring.Buffer[byte]{}
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
			rb := ring.New[int](tt.n)
			if size := rb.Len(); size != tt.n {
				t.Errorf("Len() = %v, want %v", size, tt.n)
			}
		})
	}
}

func TestBuffer_Full(t *testing.T) {
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
			rb := ring.New[int](tt.n)
			ringtest.TestFull(rb, tt.n, t)
		})
	}
}

func TestBuffer_ForcePush(t *testing.T) {
	rb := ring.New[int](5)
	ringtest.TestForcePush(rb, t)
}

func TestBuffer_Offer(t *testing.T) {
	rb := ring.New[int](5)
	ringtest.TestOffer(rb, t)
}

func TestBuffer_PushWithContext(t *testing.T) {
	ringtest.TestPushWithContext(
		func(n int) ringtest.Bufferer[int] {
			return ring.New[int](n)
		},
		t,
	)
}

func TestBuffer_Peek(t *testing.T) {
	ringtest.TestPeek(
		func(data []int) ringtest.Bufferer[int] {
			return ring.NewFrom(data)
		},
		t,
	)
}

func TestBuffer_Poll(t *testing.T) {
	ringtest.TestPoll(
		func(n int) ringtest.Bufferer[int] {
			return ring.New[int](n)
		},
		t,
	)
}

func TestBuffer_PullWithContext(t *testing.T) {
	ringtest.TestPullWithContext(
		func(n int) ringtest.Bufferer[int] {
			return ring.New[int](n)
		},
		t,
	)
}

func TestBuffer_Write(t *testing.T) {
	n := 8
	rb := ring.New[int](n)

	if rb.Len() != n {
		t.Fatalf("Len() = %v, want: %v", rb.Len(), n)
	}

	dataN := n
	data := make([]int, dataN)
	for i := range len(data) {
		data[i] = i + 1
	}

	if n, err := rb.Write(data); err != nil {
		t.Fatalf("Write(): expected nil err, got: %v", err)
	} else if n != len(data) {
		t.Fatalf("Write(): expected to write %d elements, but wrote %d", len(data), n)
	}

	if !rb.Full() {
		t.Fatal("Full(): must return true on full buffer")
	}
}

func TestBuffer_Read(t *testing.T) {
	ringtest.TestRead(
		func(n int) ringtest.Bufferer[int] {
			return ring.New[int](n)
		},
		t,
	)
}
