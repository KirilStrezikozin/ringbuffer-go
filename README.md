# ringbuffer-go

[![Go Reference](https://pkg.go.dev/badge/github.com/KirilStrezikozin/ringbuffer-go)](https://pkg.go.dev/github.com/KirilStrezikozin/ringbuffer-go) [![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg?style=flat)](https://raw.githubusercontent.com/KirilStrezikozin/ringbuffer-go/master/LICENSE) [![Tests](https://github.com/KirilStrezikozin/ringbuffer-go/actions/workflows/go.yml/badge.svg)](https://github.com/KirilStrezikozin/ringbuffer-go/actions/workflows/go.yml) [![Coverage](https://github.com/KirilStrezikozin/ringbuffer-go/wiki/coverage.svg)](https://raw.githack.com/wiki/KirilStrezikozin/ringbuffer-go/coverage.html) [![Go Report](https://goreportcard.com/badge/github.com/KirilStrezikozin/ringbuffer-go)](https://goreportcard.com/report/github.com/KirilStrezikozin/ringbuffer-go)

Blazingly fast, generic, thread-safe and lock-free, fixed-size ring (circular)
buffer implementation in Go.

- [What is a ring buffer](#what-is-a-ring-buffer)
- [Installation](#installation)
- [Usage](#usage)
- [Documentation](#documentation)
- [Benchmarks](#benchmarks)
- [License](#license)

## What is a ring buffer?

Ring buffer is a fixed-size buffer that functions as if it was connected
end-to-end. Operations like adding and removing elements are constant time
O(1), which makes this data-structure efficient for buffering data streams
with frequent reads and writes. Find more information about ring buffers
[here](https://redisson.pro/glossary/ring-buffer.html) and on
[Wikipedia](https://en.wikipedia.org/wiki/Circular_buffer).

## Installation

This package requires [Go generics](https://go.dev/blog/intro-generics)
that are available in Go [1.18](https://go.dev/dl/) and higher.
Use `go get` to install the latest version of the library.

```
$ go get -u github.com/KirilStrezikozin/ringbuffer-go
```

Import ringbuffer-go into your project:

```go
import ring "github.com/KirilStrezikozin/ringbuffer-go"
```

## Usage

TODO: note on two different ring buffers. Give a link to go reference examples.

```go
package main

import (
	"fmt"

	ring "github.com/KirilStrezikozin/ringbuffer-go"
)

func main() {
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

	// Program prints:
	// 1
	// 3
	// 4
	// 5
	// 6
	// 7
}
```

## Documentation

Full docs are available on [Go packages](https://pkg.go.dev/github.com/KirilStrezikozin/ringbuffer-go).

## Benchmarks

All competitors and ringbuffer-go itself behave similarly to buffered
[Go channels](https://gobyexample.com/channels). It is worth noting that,
because this package, [hedzr/go-ringbuf](https://pkg.go.dev/github.com/hedzr/go-ringbuf),
and [cloudfoundry/go-diodes](https://pkg.go.dev/code.cloudfoundry.org/go-diodes)
implement lock-free algorithms prioritizing throughput without [parking and
putting go-routines to sleep like Go channels do](https://github.com/golang/go/blob/master/src/runtime/chan.go),
it is safer to use the non-blocking operations they provide in highly contended
environments to avoid continuous retries during spin races.

### Parallel producers and consumers, high contention 

```txt
                                       │    sec/op    │
RingBufferKirilStrezikozinContended      30.88n ±  0%
RingBufferKirilStrezikozinContended-2    55.46n ±  2%
RingBufferKirilStrezikozinContended-4    76.53n ± 24%
RingBufferKirilStrezikozinContended-8    73.72n ±  2%
RingBufferKirilStrezikozinContended-16   76.30n ±  1%
RingBufferKirilStrezikozinContended-32   77.49n ±  1%
geomean                                  62.06n


                                       │    sec/op    │
RingBufferHedzrContended                 39.27n ±  0%
RingBufferHedzrContended-2               88.35n ± 17%
RingBufferHedzrContended-4               108.9n ±  3%
RingBufferHedzrContended-8               111.6n ±  1%
RingBufferHedzrContended-16              115.2n ±  1%
RingBufferHedzrContended-32              117.3n ±  1%
geomean                                  91.04n


                                       │    sec/op    │
DiodeContended                           66.81n ±  4%
DiodeContended-2                         61.11n ± 20%
DiodeContended-4                         63.62n ± 32%
DiodeContended-8                         66.74n ± 19%
DiodeContended-16                        88.75n ±  8%
DiodeContended-32                        106.0n ± 20%
geomean                                  73.92n


                                       │    sec/op    │
GoChanContended                          38.83n ±  1%
GoChanContended-2                        150.4n ±  1%
GoChanContended-4                        152.6n ±  2%
GoChanContended-8                        161.2n ± 17%
GoChanContended-16                       157.1n ±  1%
GoChanContended-32                       158.4n ±  1%
geomean                                  123.6n
```

### One producer and consumer

```
                                         │    sec/op    │
RingBufferKirilStrezikozinUncontended      11.19n ±  4%
RingBufferKirilStrezikozinUncontended-2    11.27n ±  2%
RingBufferKirilStrezikozinUncontended-4    14.24n ± 11%
RingBufferKirilStrezikozinUncontended-8    14.83n ±  5%
RingBufferKirilStrezikozinUncontended-16   14.84n ± 16%
RingBufferKirilStrezikozinUncontended-32   14.54n ± 21%
geomean                                    13.39n


                                         │    sec/op    │
RingBufferHedzrUncontended                 11.86n ±  4%
RingBufferHedzrUncontended-2               11.97n ±  2%
RingBufferHedzrUncontended-4               15.49n ±  5%
RingBufferHedzrUncontended-8               15.05n ±  5%
RingBufferHedzrUncontended-16              17.79n ± 13%
RingBufferHedzrUncontended-32              14.93n ±  4%
geomean                                    14.37n


                                         │    sec/op    │
DiodeUncontended                           52.28n ±  0%
DiodeUncontended-2                         47.09n ±  4%
DiodeUncontended-4                         62.28n ± 12%
DiodeUncontended-8                         57.07n ± 13%
DiodeUncontended-16                        63.84n ±  3%
DiodeUncontended-32                        61.20n ±  2%
geomean                                    56.97n


                                         │    sec/op    │
GoChanUncontended                          14.04n ± 18%
GoChanUncontended-2                        20.20n ±  5%
GoChanUncontended-4                        19.43n ±  6%
GoChanUncontended-8                        19.31n ±  4%
GoChanUncontended-16                       19.71n ±  6%
GoChanUncontended-32                       20.94n ± 37%
geomean                                    18.79n

```

### Memory usage

#### Read, write operations

```txt
GoChan                            0 B/op	       0 allocs/op
RingBufferKirilStrezikozin        0 B/op	       0 allocs/op
RingBufferHedzr                   0 B/op	       0 allocs/op
Diode                            16 B/op	       1 allocs/op
```

#### Creation

The amount of memory required to create a new ring buffer.

```txt
                                   │    sec/op    │
GoChanCreation                       38.36n ± 19%
RingBufferKirilStrezikozinCreation   21.86n ±  0%
RingBufferHedzrCreation              145.3n ± 16%
DiodeCreation                        51.27n ± 15%

                                   │    B/op    │
GoChanCreation                       112.0 ± 0%
RingBufferKirilStrezikozinCreation   16.00 ± 0%
RingBufferHedzrCreation              448.0 ± 0%
DiodeCreation                        8.000 ± 0%

                                   │ allocs/op  │
GoChanCreation                       1.000 ± 0%
RingBufferKirilStrezikozinCreation   1.000 ± 0%
RingBufferHedzrCreation              2.000 ± 0%
DiodeCreation                        1.000 ± 0%
```

Generated on a 8-core i5-10310U CPU @ 1.70GHz with:

```
$ go test -bench=. -benchtime=0.5s -count=10 -benchmem -cpu 1,2,4,8,16,32
```

Summaries were computed using [benchstat](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>.
