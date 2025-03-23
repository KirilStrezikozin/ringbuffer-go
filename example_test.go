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
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	ring "github.com/KirilStrezikozin/ringbuffer-go"
)

type Message struct {
	ID   int
	Text string
}

func ExampleNew() {
	n := 5
	rbuff := ring.New[Message](n)

	delay := time.Second
	var retries atomic.Uint32
	var wg sync.WaitGroup

	parentCtx := context.Background()

	// TODO: achieve thread-safety.
	// TODO: need a simple example using *WithContext.
	// TODO: need a simple example with producer go-routines and a consumer in
	//       the main for loop with Poll().

	// Spawn concurrent ring buffer consumers (readers).
	for i := range n {
		wg.Add(1)
		go func(k int) {
			defer wg.Done()
			for {
				// Try pulling from the ring buffer for 1 second.
				// If we are not able to pull a message within 1 second,
				// increase retries counter and try again.
				ctx, cancel := context.WithTimeout(parentCtx, delay)

				var msg Message
				pull, cancelPull := rbuff.PullWithContext(ctx, &msg)
				<-pull.Done()
				cancelPull()

				if ctx.Err() != nil {
					// Parent context cancelled, our pull did not succeed.
					retries.Add(1)
					cancel()
				} else {
					fmt.Printf("arrived to %d consumer: %v\n", k, msg)
					cancel()
					return
				}
			}
		}(i)
	}

	// Spawn concurrent ring buffer producers (writers).
	for i := range n {
		wg.Add(1)
		go func(k int) {
			msg := Message{
				ID:   k,
				Text: fmt.Sprintf("hello from %d", k),
			}
			rbuff.Push(msg)
			wg.Done()
		}(i)
	}

	wg.Wait()
	fmt.Printf("Retried %d pulls\n", retries.Load())

	// Output:
	// Retried 0 pulls
}

func ExampleNewFrom() {
	data := make([]string, 0, 3)
	data = append(data, "hello")
	data = append(data, "world")

	rbuff := ring.NewFrom(data)

	fmt.Println(rbuff.Pull())

	rbuff.Push("!")

	fmt.Println(rbuff.Pull())
	fmt.Println(rbuff.Pull())

	// Output:
	// hello
	// world
	// !
}

func ExampleBuffer_Read() {
	size := 100
	var rw io.ReadWriter = ring.New[byte](size)

	_, _ = fmt.Fprintf(rw, "Hello world!")

	for {
		b := make([]byte, 4)
		n, err := rw.Read(b)
		if n > 0 {
			fmt.Printf("%s (%d)\n", b, n)
		}
		if err == io.EOF {
			break
		}
	}

	// Output:
	// Hell (4)
	// o wo (4)
	// rld! (4)
}
