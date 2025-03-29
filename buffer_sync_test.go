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
	"errors"
	"sync"
	"testing"

	ring "github.com/KirilStrezikozin/ringbuffer-go"
)

// fullBufferer wraps all methods of [ring.Buffer] and [ring.SyncBuffer] for
// test helpers that check results of calling common methods.
type fullBufferer[V any] interface {
	Count() int
	Len() int

	Push(value V)
	Pull() V

	Peek() V
	Offer(value V) bool
	Poll() (V, bool)
}

// takePanic is a helper function that recovers a
// panic from fun and returns it as error.
func takePanic(fun func()) (err error) {
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

func testZeroSize[V any](rb fullBufferer[V], t *testing.T) {
	if count := rb.Count(); count != 0 {
		t.Fatalf("Count(): want: %v, got: %v", 0, count)
	}

	if size := rb.Len(); size != 0 {
		t.Fatalf("Count(): want: %v, got: %v", 0, size)
	}

	var value V // A dummy value.

	if ok := rb.Offer(value); ok {
		t.Fatal("Offer(): must not succeed on zero-sized buffer")
	}

	if _, ok := rb.Poll(); ok {
		t.Fatal("Poll(): must not succeed on zero-sized buffer")
	}

	if err := takePanic(func() {
		_ = rb.Pull()
	}); err == nil {
		t.Fatal("Pull(): must panic on zero-sized buffer")
	}

	if err := takePanic(func() {
		_ = rb.Peek()
	}); err == nil {
		t.Fatal("Peek(): must panic on zero-sized buffer")
	}

	if err := takePanic(func() {
		rb.Push(value)
	}); err == nil {
		t.Fatal("Push(): must panic on zero-sized buffer")
	}
}

func TestSyncBuffer_ZeroValue(t *testing.T) {
	rb := ring.SyncBuffer[int]{}
	testZeroSize(&rb, t)
}

func TestNewSync_ZeroSize(t *testing.T) {
	type Msg struct{}

	rb := ring.NewSync[Msg](0)
	testZeroSize(rb, t)
}
