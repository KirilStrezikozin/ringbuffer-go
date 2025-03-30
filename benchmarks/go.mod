module github.com/KirilStrezikozin/ringbuffer-go/benchmarks

go 1.23.3

replace github.com/KirilStrezikozin/ringbuffer-go => ../

require (
	code.cloudfoundry.org/go-diodes v0.0.0-20250324121313-75aea42a1fc3
	github.com/KirilStrezikozin/ringbuffer-go v0.0.0-00010101000000-000000000000
	github.com/hedzr/go-ringbuf/v2 v2.2.1
)

require golang.org/x/sys v0.31.0 // indirect
