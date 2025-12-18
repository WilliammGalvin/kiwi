package main

import (
	"flag"
	"log"
	"time"

	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/data"
	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/engine"
	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/reader"
	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/transport"
)

var VerifiedCSVBarHeaders = []string{
	"Date", "Close/Last", "Volume", "Open", "High", "Low",
}

func main() {
	dataDirPath := flag.String("dataDir", "./data", "Path to data directory")
	interval := flag.Int("interval", 1000, "Broadcast interval delay in ms")
	broadcastAddr := flag.String("broadcastAddr", "localhost:8080", "Market broadcast service address to connect via TCP")
	flag.Parse()

	broadcastIntervalMs := time.Duration(*interval) * time.Millisecond
	log.Printf("Broadcasting at a rate of %v\n", broadcastIntervalMs)

	dataManager, err := data.NewDataManager(*dataDirPath)
	if err != nil {
		log.Fatalf("Error initializing data manager: %v\n", err)
	}

	symbols, err := dataManager.CollectSymbols()
	if err != nil {
		log.Fatalf("Error collecting symbols: %v\n", err)
	}

	readers := make(map[string]*reader.CSVReader)
	for _, sym := range symbols {
		symPath := dataManager.GetSymbolPath(sym)
		r := reader.NewCSVReader(symPath)

		if err := r.OpenFile(); err != nil {
			continue
		}

		if _, err := r.ReadHeaders(); err != nil {
			r.CloseFile()
			continue
		}

		if !r.VerifyHeaders(VerifiedCSVBarHeaders) {
			r.CloseFile()
			continue
		}

		readers[sym] = r
	}

	defer func() {
		for _, reader := range readers {
			reader.CloseFile()
		}
	}()

	log.Println("Stocks loaded:")
	i := 1
	for sym := range readers {
		log.Printf("%v. %s\n", i, sym)
		i++
	}

	client := transport.NewBroadcastClient(*broadcastAddr)
	client.Connect()

	schedulerEngine := engine.NewSchedulerEngine(broadcastIntervalMs, readers, client)
	schedulerEngine.Start()
	defer schedulerEngine.Shutdown()
}
