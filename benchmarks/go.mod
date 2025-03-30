module github.com/KirilStrezikozin/ringbuffer-go/benchmarks

go 1.23.3

replace github.com/KirilStrezikozin/ringbuffer-go => ../

require (
	github.com/KirilStrezikozin/ringbuffer-go v0.0.0-00010101000000-000000000000
	github.com/hedzr/go-ringbuf/v2 v2.2.1
)

require golang.org/x/sys v0.30.0 // indirect
