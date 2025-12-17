package main

import (
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/data"
	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/reader"
)

var VerifiedCSVBarHeaders = []string{
	"Date", "Close", "Last", "Volume", "Open", "High", "Low",
}

func main() {
	dataDirPath := flag.String("dataDir", "./data", "Path to data directory")
	interval := flag.Int("interval", 1000, "Broadcast interval delay in ms")
	flag.Parse()

	broadcastIntervalMs := time.Duration(*interval) * time.Millisecond
	fmt.Printf("Broadcasting at a rate of %v\n", broadcastIntervalMs)

	dataManager, err := data.NewDataManager(*dataDirPath)
	if err != nil {
		log.Fatalf("Error initializing data manager: %v\n", err)
	}

	symbols, err := dataManager.CollectSymbols()
	if err != nil {
		log.Fatalf("Error collecting symbols: %v\n", err)
	}

	readers := []reader.CSVReader{}
	for _, sym := range symbols {
		if !dataManager.HasSymbol(sym) {
			continue
		}

		symPath := dataManager.GetSymbolPath(sym)
		reader := reader.NewCSVReader(symPath)
		err := reader.OpenFile()
		if err != nil {
			continue
		}

		reader.ReadHeaders()
		reader.VerifyHeaders(VerifiedCSVBarHeaders)
		readers = append(readers, *reader)
	}

	fmt.Println("Stocks loaded:")
	for i, sym := range symbols {
		fmt.Printf("%v. %s\n", i+1, sym)
	}

	defer func() {
		for _, reader := range readers {
			reader.CloseFile()
		}
	}()
}
