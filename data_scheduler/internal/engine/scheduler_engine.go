package engine

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/reader"
	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/transport"
	"github.com/WilliammGalvin/kiwi/data_scheduler/pkg/models"
)

type WorkerReader struct {
	Symbol  string
	Context context.Context
	Cancel  context.CancelFunc
	Reader  reader.CSVReader
}

type SchedulerEngine struct {
	Interval  time.Duration
	Client    *transport.BroadcastClient
	workers   []WorkerReader
	waitGroup sync.WaitGroup
}

func NewSchedulerEngine(interval time.Duration, readers map[string]*reader.CSVReader, client *transport.BroadcastClient) *SchedulerEngine {
	workers := []WorkerReader{}
	for sym, r := range readers {
		ctx, cancel := context.WithCancel(context.Background())
		workers = append(workers, WorkerReader{
			Symbol:  sym,
			Context: ctx,
			Cancel:  cancel,
			Reader:  *r,
		})
	}

	return &SchedulerEngine{
		Interval: interval,
		Client:   client,
		workers:  workers,
	}
}

func (engine *SchedulerEngine) parseBarPacketFromCSVRow(row []string) (*models.BarPacket, error) {
	if len(row) != 6 {
		return nil, fmt.Errorf("bar data row list should have length of 6")
	}

	timestamp_str := row[0]
	close_str := row[1]
	volume_str := row[2]
	open_str := row[3]
	high_str := row[4]
	low_str := row[5]

	timestamp_i64, err := strconv.ParseInt(timestamp_str, 10, 64)
	if err != nil {
		return nil, err
	}

	open_f64, err := strconv.ParseFloat(open_str, 64)
	if err != nil {
		return nil, err
	}

	close_f64, err := strconv.ParseFloat(close_str, 64)
	if err != nil {
		return nil, err
	}

	high_f64, err := strconv.ParseFloat(high_str, 64)
	if err != nil {
		return nil, err
	}

	low_f64, err := strconv.ParseFloat(low_str, 64)
	if err != nil {
		return nil, err
	}

	volume_f64, err := strconv.ParseInt(volume_str, 10, 64)
	if err != nil {
		return nil, err
	}

	return models.NewBarPacket(timestamp_i64, open_f64, close_f64, high_f64, low_f64, volume_f64), nil
}

func (engine *SchedulerEngine) runWorker(worker *WorkerReader) {
	defer engine.waitGroup.Done()
	defer worker.Reader.CloseFile()

	ticker := time.NewTicker(engine.Interval)
	defer ticker.Stop()

	for {
		if worker.Context.Err() != nil {
			return
		}

		time.Sleep(engine.Interval)

		row, err := worker.Reader.ReadRow()
		if err != nil {
			log.Printf("Worker %s error reading: %v\n", worker.Symbol, err)
			return
		}

		if row == nil || worker.Reader.HasReachedEOF {
			log.Printf("Worker has reached the end of %s history\n", worker.Symbol)
			return
		}

		packet, err := engine.parseBarPacketFromCSVRow(row)
		if err != nil {
			log.Fatalf("Error occured parsing bar packet from row: %v\n", err)
		}

		if !engine.Client.IsConnected() {
			log.Fatalf("Client is not connected\n")
		}

		if err := engine.Client.Send(packet); err != nil {
			log.Printf("Error sending packet to client: %v\n", err)
		}

		log.Printf("Bar packet for %s sent successfully", worker.Symbol)
	}
}

func (engine *SchedulerEngine) Start() {
	for i := range engine.workers {
		engine.waitGroup.Add(1)
		go engine.runWorker(&engine.workers[i])
	}

	engine.waitGroup.Wait()
}

func (engine *SchedulerEngine) Shutdown() {
	for _, w := range engine.workers {
		w.Cancel()
	}

	engine.waitGroup.Wait()
}
