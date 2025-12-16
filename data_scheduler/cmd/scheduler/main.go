package main

import (
	"encoding/csv"
	"path/filepath"
	"fmt"
	"os"
	"time"
	"net"
	"strings"
)

var preloadCount = 10
var entryCounts = make(map[string]int)
var stockData = make(map[string][][]string)  

func fetchEntries(sourceDir string, symbol string, fetchCount int) {
	path := filepath.Join(sourceDir, symbol + ".csv")

	file, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	
	// Skips to next line (terribly inefficient)
	for i := 0; i < entryCounts[symbol]; i++ {
		_, err := reader.Read()
		if err != nil {
			return
		}
	}

	for i := 0; i < fetchCount; i++ {
		record, err := reader.Read()
		if err != nil {
			break
		}

		stockData[symbol] = append(stockData[symbol], record)
		entryCounts[symbol]++
	}
}

func gatherStockSymbols(source string) {
	entries, err := os.ReadDir(source)
	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		ext := filepath.Ext(name)
		symbol := name[:len(name) - len(ext)]

		if ext != ".csv" {
			continue
		}

		stockData[symbol] = [][]string{}
	}
}

func main() {
	dataPath := "../../data/"
	serverAddr := "localhost:8080"

	conn, err := net.Dial("tcp", serverAddr)
	if err != nil {
		panic(fmt.Sprintf("Failed to connect to server: %v", err))
	}
	defer conn.Close()
	fmt.Printf("Connected to %s\n", serverAddr)
	
	gatherStockSymbols(dataPath)
	for range preloadCount {
		for sym := range stockData {
			fetchEntries(dataPath, sym, 1)
		}
	}

	for {
		for sym := range stockData {
			if len(stockData[sym]) < preloadCount {
				fetchEntries(dataPath, sym, preloadCount - len(stockData[sym]))

				if len(stockData[sym]) <= 0 {
					delete(stockData, sym)
					continue
				}
			}

			front := stockData[sym][0]

			packet := fmt.Sprintf("%s,%s\n", sym, strings.Join(front, ","))
			_, err := conn.Write([]byte(packet))
			if err != nil {
				fmt.Printf("Failed to send data: %v\n", err)
				return
			}

			fmt.Printf("Sent: %s", packet)
			stockData[sym] = stockData[sym][1:]
		}

		time.Sleep(time.Second)
	}	
}
