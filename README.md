# solaman-go

`solaman-go` is a Go client for the Solarman V5 protocol used by compatible WiFi data loggers.
It sends Modbus RTU read requests through the logger's local TCP interface, normally port 8899.

The module only implements the generic transport. Inverter-specific register maps and value decoding belong to its consumers.

## Usage

```go
client, err := solarman.New("192.0.2.2", 8899, loggerSerial, 10*time.Second)
if err != nil {
	return err
}

data, err := client.ReadHoldingRegisters(context.Background(), 1, 86, 2)
```

The logger serial number is part of every Solarman V5 request. It is the numerical serial number of the WiFi logger, not the inverter serial number.

## Supported operations

- Read holding registers, Modbus function `0x03`
- Read input registers, Modbus function `0x04`

The module is read-only and does not implement Modbus write operations.

## Compatibility

Compatibility depends on the installed logger exposing the Solarman V5 protocol over TCP. Inverter register maps vary by manufacturer and model.

## License

[MIT](LICENSE)
