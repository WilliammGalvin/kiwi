package main

import (
	"encoding/binary"
	"log"
	"math"
	"net"
	"time"
)

const (
	listenAddr = ":8080"
	retryDelay = 2 * time.Second
	packetSize = 48
)

func main() {
	var listener net.Listener
	var err error

	for {
		listener, err = net.Listen("tcp", listenAddr)
		if err == nil {
			break
		}

		log.Printf("Failed to start server: %v\n", err)
		log.Printf("Retrying in %v...\n", retryDelay)
		time.Sleep(retryDelay)
	}

	defer listener.Close()
	log.Printf("Server listening on %s\n", listenAddr)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}

		go handleClient(conn)
	}
}

func handleClient(conn net.Conn) {
	defer conn.Close()
	clientAddr := conn.RemoteAddr().String()
	log.Printf("Client connected: %s\n", clientAddr)

	buf := make([]byte, packetSize)

	for {
		_, err := readFull(conn, buf)
		if err != nil {
			log.Printf("Client %s disconnected: %v\n", clientAddr, err)
			return
		}

		timestamp := int64(binary.LittleEndian.Uint64(buf[0:8]))
		open := math.Float64frombits(binary.LittleEndian.Uint64(buf[8:16]))
		close := math.Float64frombits(binary.LittleEndian.Uint64(buf[16:24]))
		high := math.Float64frombits(binary.LittleEndian.Uint64(buf[24:32]))
		low := math.Float64frombits(binary.LittleEndian.Uint64(buf[32:40]))
		volume := int64(binary.LittleEndian.Uint64(buf[40:48]))

		log.Printf("[%s] %s | ts=%d open=%.2f close=%.2f high=%.2f low=%.2f vol=%d",
			time.Now().Format("15:04:05"),
			clientAddr,
			timestamp, open, close, high, low, volume,
		)
	}
}

func readFull(conn net.Conn, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := conn.Read(buf[total:])
		if err != nil {
			return total, err
		}

		total += n
	}

	return total, nil
}

