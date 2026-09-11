package solarman

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

func TestNewValidatesInput(t *testing.T) {
	tests := []struct {
		name    string
		host    string
		port    int
		serial  uint32
		timeout time.Duration
	}{
		{name: "missing host", port: 8899, serial: 1, timeout: time.Second},
		{name: "invalid port", host: "localhost", port: 0, serial: 1, timeout: time.Second},
		{name: "missing serial", host: "localhost", port: 8899, timeout: time.Second},
		{name: "invalid timeout", host: "localhost", port: 8899, serial: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(tt.host, tt.port, tt.serial, tt.timeout); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}

	client, err := New("localhost", 8899, 1, time.Second, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
}

func TestReadHoldingRegistersReusesConnection(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	const serial = 3875738533
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()

		for request := range 2 {
			frame, err := readFrame(conn)
			if err != nil {
				done <- err
				return
			}
			if got := binary.LittleEndian.Uint32(frame[7:11]); got != serial {
				done <- errors.New("request has wrong logger serial")
				return
			}
			modbus := frame[26 : len(frame)-2]
			want := []byte{1, 3, 0, 86, 0, 2}
			if string(modbus[:6]) != string(want) || !validCRC(modbus) {
				done <- errors.New("request has wrong Modbus payload")
				return
			}

			response := []byte{1, 3, 4, 0, 0, 2, 48}
			response = append(response, crc(response)...)
			response = append(response, 0, 0)
			if request == 0 {
				if _, err := conn.Write(responseFrame(serial, frame[5]+1, response)); err != nil {
					done <- err
					return
				}
			}
			if _, err := conn.Write(responseFrame(serial, frame[5], response)); err != nil {
				done <- err
				return
			}
		}
		done <- nil
	}()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := net.LookupPort("tcp", port)
	if err != nil {
		t.Fatal(err)
	}

	var messages []string
	client, err := New(host, portNumber, serial, time.Second, WithLogger(func(format string, args ...any) {
		messages = append(messages, fmt.Sprintf(format, args...))
	}))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	for range 2 {
		value, err := client.ReadHoldingRegisters(context.Background(), 1, 86, 2)
		if err != nil {
			t.Fatal(err)
		}
		if want := []byte{0, 0, 2, 48}; string(value) != string(want) {
			t.Fatalf("value: got %x, want %x", value, want)
		}
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if len(messages) != 5 || !strings.HasPrefix(messages[0], "send ") || !strings.HasPrefix(messages[1], "recv ") || !strings.HasPrefix(messages[2], "recv ") || !strings.HasPrefix(messages[3], "send ") || !strings.HasPrefix(messages[4], "recv ") {
		t.Fatalf("unexpected log messages: %v", messages)
	}
}

func TestReadInputRegisters(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	const serial = 3875738533
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()

		frame, err := readFrame(conn)
		if err != nil {
			done <- err
			return
		}
		modbus := frame[26 : len(frame)-2]
		if got, want := modbus[1], byte(0x04); got != want {
			done <- fmt.Errorf("Modbus function: got %d, want %d", got, want)
			return
		}

		response := []byte{1, 4, 2, 0, 1}
		response = append(response, crc(response)...)
		if _, err := conn.Write(responseFrame(serial, frame[5], response)); err != nil {
			done <- err
			return
		}
		done <- nil
	}()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := net.LookupPort("tcp", port)
	if err != nil {
		t.Fatal(err)
	}

	client, err := New(host, portNumber, serial, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	value, err := client.ReadInputRegisters(context.Background(), 1, 86, 1)
	if err != nil {
		t.Fatal(err)
	}
	if want := []byte{0, 1}; string(value) != string(want) {
		t.Fatalf("value: got %x, want %x", value, want)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReadHoldingRegistersReconnects(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	const serial = 3875738533
	done := make(chan error, 1)
	go func() {
		for connection := range 2 {
			conn, err := listener.Accept()
			if err != nil {
				done <- err
				return
			}

			frame, err := readFrame(conn)
			if err != nil {
				_ = conn.Close()
				done <- err
				return
			}
			response := []byte{1, 3, 4, 0, 0, 2, 48}
			response = append(response, crc(response)...)
			response = append(response, 0, 0)
			_, err = conn.Write(responseFrame(serial, frame[5], response))
			_ = conn.Close()
			if err != nil {
				done <- err
				return
			}
			if connection == 0 {
				continue
			}
			done <- nil
			return
		}
	}()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := net.LookupPort("tcp", port)
	if err != nil {
		t.Fatal(err)
	}

	client, err := New(host, portNumber, serial, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	if _, err := client.ReadHoldingRegisters(context.Background(), 1, 86, 2); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ReadHoldingRegisters(context.Background(), 1, 86, 2); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestReadRegistersHonorsContextCancellation(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	requestReceived := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			done <- err
			return
		}
		defer conn.Close()

		if _, err := readFrame(conn); err != nil {
			done <- err
			return
		}
		close(requestReceived)

		buffer := make([]byte, 1)
		if _, err := conn.Read(buffer); !errors.Is(err, io.EOF) {
			done <- fmt.Errorf("read after client cancellation: %w", err)
			return
		}
		done <- nil
	}()

	host, port, err := net.SplitHostPort(listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	portNumber, err := net.LookupPort("tcp", port)
	if err != nil {
		t.Fatal(err)
	}

	client, err := New(host, portNumber, 1, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	readDone := make(chan error, 1)
	go func() {
		_, err := client.ReadHoldingRegisters(ctx, 1, 86, 1)
		readDone <- err
	}()

	select {
	case <-requestReceived:
	case err := <-done:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("server did not receive the request")
	}
	cancel()

	select {
	case err := <-readDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("read error: got %v, want context canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read did not return after context cancellation")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestValidateResponseChecksum(t *testing.T) {
	frame := responseFrame(1, 1, []byte{1, 3, 2, 0, 1, 0, 0})
	frame[len(frame)-2]++

	if _, err := validateResponse(frame, 1, 1); err == nil {
		t.Fatal("expected checksum error")
	}
}

func TestValidateModbusException(t *testing.T) {
	response := []byte{1, 0x83, 2}
	response = append(response, crc(response)...)

	_, err := validateModbusResponse(response, 1, 3, 1)
	var exception *ModbusExceptionError
	if !errors.As(err, &exception) {
		t.Fatalf("error: got %v, want ModbusExceptionError", err)
	}
	if exception.Code != 2 {
		t.Fatalf("exception code: got %d, want 2", exception.Code)
	}
}

func responseFrame(serial uint32, sequence byte, modbus []byte) []byte {
	payload := make([]byte, 14, 14+len(modbus))
	payload[0] = 0x02
	payload = append(payload, modbus...)

	frame := []byte{startByte, byte(len(payload)), byte(len(payload) >> 8), 0x10, responseControlCode, sequence, 0x00}
	serialBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(serialBytes, serial)
	frame = append(frame, serialBytes...)
	frame = append(frame, payload...)
	frame = append(frame, checksum(frame[1:]), endByte)

	return frame
}
