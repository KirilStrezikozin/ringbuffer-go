# ringbuffer-go
Fast, generic, thread-safe and fixed-size ring (circular) buffer implementation
in Go.

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

Full docs are available on [Go packages](TODO).

## Benchmarks

TODO.

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) or <http://www.apache.org/licenses/LICENSE-2.0>.
