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
	"time"

	ring "github.com/KirilStrezikozin/ringbuffer-go"
)

type Message struct {
	ID   int
	Text string
}

func Example() {
	rb := ring.NewSync[int](5)

	rb.Push(1)
	rb.Push(2)
	rb.Push(3)

	// Ring buffer state is:
	// [1] [2] [3] [ ] [ ]

	fmt.Println(rb.Pull())

	// Ring buffer state is:
	// [ ] [2] [3] [ ] [ ]

	rb.Push(4)
	rb.Push(5)
	rb.Push(6)

	// Ring buffer state is:
	// [6] [2] [3] [4] [5]

	rb.ForcePush(7)

	// Ring buffer state is:
	// [6] [7] [3] [4] [5]

	for {
		value, ok := rb.Poll()
		if !ok {
			break
		}
		fmt.Println(value)
	}

	// Output:
	// 1
	// 3
	// 4
	// 5
	// 6
	// 7
}

// Create a ring buffer that is safe to push/pull from concurrently.
// Launch a go-routine that pulls elements from the ring buffer, blocking if
// there are no elements to pull. Launch another go-routine that inserts
// elements into the ring buffer, without blocking if the buffer is full.
func ExampleNewSync() {
	rb := ring.NewSync[Message](2)

	total := 4
	var wg sync.WaitGroup

	// Spawn a concurrent ring buffer consumer (reader). Multiple such
	// consumers can safely execute in parallel when using [ring.SyncBuffer].
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for {
			if n == total {
				return
			}
			msg := rb.Pull()
			fmt.Println(msg.Text)
			n++
		}
	}()

	// Spawn a concurrent ring buffer producer (writer). You can safely launch
	// as many such producers as you need when using [ring.SyncBuffer].
	wg.Add(1)
	go func() {
		defer wg.Done()
		n := 0
		for n < total {
			msg := Message{
				ID:   n + 1,
				Text: fmt.Sprintf("hello from %d", n+1),
			}

			if ok := rb.Offer(msg); !ok {
				// We can abort our go-routine here, or perform any kind of
				// work. In this case, we simply retry.
				//
				// Note that we created a ring buffer with size 2. This means
				// that after successfully pushing 2 elements, the buffer
				// becomes full. We wait a small delay to give our consumer
				// above the ability to pull values from the ring buffer,
				// effectively emptying it.
				time.Sleep(50 * time.Millisecond)
				continue
			}

			n++
		}
	}()

	wg.Wait()

	// Output:
	// hello from 1
	// hello from 2
	// hello from 3
	// hello from 4
}

// Create ring buffer with initially pre-populated data.
func ExampleNewFrom() {
	data := make([]string, 0, 3)
	data = append(data, "hello")
	data = append(data, "world")

	rbuff := ring.NewFrom(data)

	// [ring.NewFrom] does not retain data that was given to it.
	data[0] = "not hello?"

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

// Drain a ring buffer.
func Example_drain() {
	// Drain (remove all elements from) a ring buffer.
	// Same can be done with ring.SyncBuffer.

	buff := []byte("hello world")
	rb := ring.NewFrom(buff)

	fmt.Println(rb.Full())

	for {
		if _, ok := rb.Poll(); !ok {
			break
		}
	}

	fmt.Println(rb.Count())
	fmt.Println(rb.Len())

	// Output:
	// true
	// 0
	// 11
}

// Push elements into a ring buffer with a context. Push fails after a timeout.
func ExampleSyncBuffer_PushWithContext() {
	rb := ring.NewSync[int](3)

	total := 5
	start := time.Now()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range total {
			// Give each push 1 second to complete. If the buffer is full,
			// the pull will cancel the context it returned after 1 second.
			parent, cancelParent := context.WithTimeout(context.Background(), time.Second)

			ctx, cancel := rb.PushWithContext(parent, i+1)

			<-ctx.Done()
			if parent.Err() != nil {
				fmt.Printf("Push %d at second %d: timeout\n", i+1, int(time.Since(start).Seconds()))
			} else {
				fmt.Printf("Push %d at second %d: ok\n", i+1, int(time.Since(start).Seconds()))
			}

			cancel()
			cancelParent()
		}
	}()

	wg.Wait()

	// Output:
	// Push 1 at second 0: ok
	// Push 2 at second 0: ok
	// Push 3 at second 0: ok
	// Push 4 at second 1: timeout
	// Push 5 at second 2: timeout
}
