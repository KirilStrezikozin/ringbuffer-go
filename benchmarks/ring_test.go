package benchmarks

import (
	"crypto/rand"
	"sync"
	"testing"
	"time"

	"code.cloudfoundry.org/go-diodes"
	"github.com/KirilStrezikozin/ringbuffer-go"
	"github.com/hedzr/go-ringbuf/v2"
)

type Message struct {
	ID   int
	Text string
}

var (
	diodeNopAlerter = diodes.AlertFunc(func(int) {})
	randData        = randDataGen()
)

func BenchmarkGoChanContended(b *testing.B) {
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

func BenchmarkRingBufferKirilStrezikozinContended(b *testing.B) {
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

func BenchmarkRingBufferHedzrContended(b *testing.B) {
	rb := ringbuf.New[Message](2)
	msg := Message{ID: 1, Text: "hello world"}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			rb.Enqueue(msg) //nolint:errcheck
			rb.Dequeue()    //nolint:errcheck
		}
	})
}

func BenchmarkDiodeContended(b *testing.B) {
	d := diodes.NewWaiter(diodes.NewManyToOne(1000, diodeNopAlerter))
	msg := Message{ID: 1, Text: "hello world"}
    var dummy Message
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d.Set(diodes.GenericDataType(&msg))
			dummy = *(*Message)(d.Next())
		}
	})
    if dummy != msg {
        b.Fail()
    }
}

func BenchmarkGoChanUncontended(b *testing.B) {
	c := make(chan []byte, 100)
	var wg sync.WaitGroup
	wg.Add(1)

	done := make(chan struct{})
	go func() {
		wg.Done()
		for {
			select {
			case <-c:
			case <-done:
				wg.Done()
				return
			}
		}
	}()

	wg.Wait()
	wg.Add(1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := randData(i)
		select {
		case c <- *data:
		default:
			select {
			case <-c:
			default:
			}
		}
	}

	b.StopTimer()
	close(done)
	wg.Wait()
}

func BenchmarkRingBufferKirilStrezikozinUncontended(b *testing.B) {
	rb := ring.NewSync[[]byte](100)

	var wg sync.WaitGroup
	wg.Add(1)

	done := make(chan struct{})
	go func() {
		wg.Done()
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				rb.Poll()
				time.Sleep(time.Microsecond)
			}
		}
	}()

	wg.Wait()
	wg.Add(1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := randData(i)
		rb.ForcePush(*data)
	}

	b.StopTimer()
	close(done)
	wg.Wait()
}

func BenchmarkRingBufferHedzrUncontended(b *testing.B) {
	rb := ringbuf.New[[]byte](100)

	var wg sync.WaitGroup
	wg.Add(1)

	done := make(chan struct{})
	go func() {
		wg.Done()
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				rb.Dequeue() //nolint:errcheck
				time.Sleep(time.Microsecond)
			}
		}
	}()

	wg.Wait()
	wg.Add(1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := randData(i)
		rb.Enqueue(*data) //nolint:errcheck
	}

	b.StopTimer()
	close(done)
	wg.Wait()
}

func BenchmarkDiodeUncontended(b *testing.B) {
	d := diodes.NewPoller(diodes.NewOneToOne(100, diodeNopAlerter))
	var wg sync.WaitGroup
	wg.Add(1)

	done := make(chan struct{})
	go func() {
		wg.Done()
		for {
			select {
			case <-done:
				wg.Done()
				return
			default:
				d.TryNext()
				time.Sleep(time.Microsecond)
			}
		}
	}()

	wg.Wait()
	wg.Add(1)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		data := randData(i)
		d.Set(diodes.GenericDataType(data))
	}

	b.StopTimer()
	close(done)
	wg.Wait()
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

func BenchmarkDiodeCreation(b *testing.B) {
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			d := diodes.NewOneToOne(1, diodeNopAlerter)
			if _, ok := d.TryNext(); ok {
				b.Fail()
			}
		}
	})
}

func randDataGen() func(int) *[]byte {
	// https://github.com/cloudfoundry/go-diodes/blob/main/benchmarks_test.go

	var data [][]byte

	for j := 0; j < 5; j++ {
		buffer := make([]byte, 100)
		rand.Read(buffer) //nolint:errcheck
		data = append(data, buffer)
	}

	return func(i int) *[]byte {
		return &data[i%len(data)]
	}
}
