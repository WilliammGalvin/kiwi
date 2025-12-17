package main

import (
	"bufio"
	"log"
	"net"
	"time"
)

func main() {
	listener, err := net.Listen("tcp", ":8080")
	if err != nil {
		log.Fatal("Failed to start server:", err)
	}
	defer listener.Close()

	log.Println("Server listening on :8080")

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
	log.Printf("New connection from: %s", clientAddr)

	reader := bufio.NewReader(conn)

	for {
		data, err := reader.ReadBytes('\n')
		if err != nil {
			log.Printf("Client %s disconnected: %v", clientAddr, err)
			return
		}

		log.Printf("[%s] %s: %s", time.Now().Format("15:04:05"), clientAddr, string(data))
		conn.Write([]byte("ACK\n"))
	}
}
