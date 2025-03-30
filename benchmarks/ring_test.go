package benchmarks

import (
	"testing"

	"github.com/KirilStrezikozin/ringbuffer-go"
	"github.com/hedzr/go-ringbuf/v2"
)

type Message struct {
	ID   int
	Text string
}

func BenchmarkGoChanSync(b *testing.B) {
	ch := make(chan Message, 1)
	msg := Message{ID: 1, Text: "hello world"}
	var dummy Message
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch <- msg
			dummy = <-ch
		}
	})
	if dummy != msg {
		b.Fail()
	}
}

func BenchmarkRingBufferKirilStrezikozinSync(b *testing.B) {
	rb := ring.NewSync[Message](1)
	msg := Message{ID: 1, Text: "hello world"}
	var dummy Message
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rb.Push(msg)
			dummy = rb.Pull()
		}
	})
	if dummy != msg {
		b.Fail()
	}
}

func BenchmarkRingBufferHedzrSync(b *testing.B) {
	rb := ringbuf.New[Message](2)
	msg := Message{ID: 1, Text: "hello world"}
	var err error
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err = rb.Enqueue(msg)
			_, err = rb.Dequeue()
		}
	})
	if err != nil {
		b.Fail()
	}
}

func BenchmarkGoChanCreation(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			ch := make(chan int, 1)
			if cap(ch) != 1 {
				b.Fail()
			}
		}
	})
}

func BenchmarkRingBufferKirilStrezikozinCreation(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rb := ring.NewSync[int](1)
			if rb.Len() != 1 {
				b.Fail()
			}
		}
	})
}

func BenchmarkRingBufferHedzrCreation(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rb := ringbuf.New[int](1)
			if rb.Cap() != 1 {
				b.Fail()
			}
		}
	})
}
