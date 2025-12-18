package transport

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/WilliammGalvin/kiwi/data_scheduler/pkg/models"
)

type BroadcastClient struct {
	addr        string
	conn        net.Conn
	lock        sync.Mutex
	retryDelay  time.Duration
	maxRetries  int
	isConnected bool
}

func NewBroadcastClient(addr string) *BroadcastClient {
	return &BroadcastClient{
		addr:       addr,
		retryDelay: 2 * time.Second,
		maxRetries: -1, // Retry forever
	}
}

func NewBroadcastClientWithOptions(addr string, retryDelay time.Duration, maxRetries int) *BroadcastClient {
	return &BroadcastClient{
		addr:       addr,
		retryDelay: retryDelay,
		maxRetries: maxRetries,
	}
}

func (client *BroadcastClient) Connect() error {
	attempt := 0

	for {
		log.Printf("Attempting to connect to %s (attempt %d)...\n", client.addr, attempt)

		conn, err := net.Dial("tcp", client.addr)
		if err == nil {
			client.lock.Lock()
			client.conn = conn
			client.isConnected = true
			client.lock.Unlock()

			log.Printf("Connected to %s\n", client.addr)
			return nil
		}

		log.Printf("Connection failed: %v\n", err)

		if client.maxRetries >= 0 && attempt >= client.maxRetries {
			return fmt.Errorf("failed to connect after %d attempts: %w", attempt, err)
		}

		log.Printf("Retrying in %v...", client.retryDelay)
		time.Sleep(client.retryDelay)
		attempt++
	}
}

func (client *BroadcastClient) Send(packet *models.BarPacket) error {
	client.lock.Lock()
	defer client.lock.Unlock()

	if client.conn == nil || !client.isConnected {
		return fmt.Errorf("not connected")
	}

	_, err := packet.WriteTo(client.conn)
	if err != nil {
		client.isConnected = false
		log.Printf("Client has disconnected: %v\n", err)
		return err
	}

	return nil
}

func (client *BroadcastClient) Close() error {
	if client.conn != nil {
		return client.conn.Close()
	}

	return nil
}

func (client *BroadcastClient) IsConnected() bool {
	client.lock.Lock()
	defer client.lock.Unlock()
	return client.isConnected
}
