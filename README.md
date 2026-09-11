# solarman-go

`solarman-go` is a Go client for the local Solarman V5 TCP transport exposed by compatible data loggers, normally on port `8899`.
It forwards Modbus RTU read requests through the logger to the connected inverter.

The module implements transport only. Register maps, data types, scaling and value decoding are specific to the connected inverter and belong to the consumer.

## Requirements

- A Solarman V5-compatible data logger reachable on the local network
- The logger's numerical serial number
- The Modbus unit ID and register map for the connected inverter

The serial number in each request is the **logger serial number**, not the inverter serial number. It is commonly shown on the logger's local status page or label.

Compatibility depends on the logger exposing Solarman V5 over TCP. This library is not tied to a particular inverter manufacturer, but a register map that works with one inverter model is not necessarily valid for another.

## Usage

```go
package main

import (
	"context"
	"log"
	"time"

	"github.com/404GamerNotFound/solarman-go"
)

func main() {
	client, err := solarman.New(
		"192.0.2.2",
		8899,
		123456789,
		10*time.Second,
		solarman.WithLogger(log.Printf), // optional transport tracing
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	raw, err := client.ReadHoldingRegisters(ctx, 1, 86, 2)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("received %x", raw)
}
```

`ReadHoldingRegisters` and `ReadInputRegisters` return raw Modbus response bytes. Decode these according to the documentation for the specific inverter. In particular, verify whether register addresses are zero- or one-based and which byte order, signedness and scaling a value uses.

## Supported operations

- Read holding registers, Modbus function `0x03`
- Read input registers, Modbus function `0x04`

The module is read-only. It does not implement write requests, coils or discrete inputs. A read request must contain between 1 and 125 registers.

## Connections and timeouts

A `Client` reuses its TCP connection for subsequent requests. Close it with `Close` when it is no longer needed. Calls on a client are serialized, so one client is safe to share between goroutines, although requests are processed one at a time.

The timeout passed to `New` limits dialing and each transport operation. An earlier context deadline takes precedence. If a reused connection fails because of a transport error, the client closes it, reconnects and retries the request once.

## Debug logging

Pass `solarman.WithLogger(log.Printf)` to `New` to log sent and received Solarman V5 frames in hexadecimal. This is useful for troubleshooting transport issues. Trace logs include device addresses and logger serial numbers, so avoid publishing them without reviewing their contents.

## Errors

`New` validates the host, TCP port, logger serial number and timeout. Read methods validate the register count. Errors from the network, context deadline, protocol framing and Modbus response are returned to the caller with context. Modbus exception responses can be inspected with `errors.As` and `*solarman.ModbusExceptionError`.

## License

[MIT](LICENSE)
