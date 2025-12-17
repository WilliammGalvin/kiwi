package engine

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/WilliammGalvin/kiwi/data_scheduler/internal/reader"
)

type WorkerReader struct {
	Symbol  string
	Context context.Context
	Cancel  context.CancelFunc
	Reader  reader.CSVReader
}

type BroadcastEngine struct {
	Interval  time.Duration
	workers   []WorkerReader
	waitGroup sync.WaitGroup
}

func NewBroadcastEngine(interval time.Duration, readers map[string]*reader.CSVReader) *BroadcastEngine {
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

	return &BroadcastEngine{
		Interval: interval,
		workers:  workers,
	}
}

func (engine *BroadcastEngine) runWorker(worker *WorkerReader) {
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

		log.Printf("%s %d: %v\n", worker.Symbol, worker.Reader.Index, row)
	}
}

func (engine *BroadcastEngine) Start() {
	for i := range engine.workers {
		engine.waitGroup.Add(1)
		go engine.runWorker(&engine.workers[i])
	}

	engine.waitGroup.Wait()
}

func (engine *BroadcastEngine) Shutdown() {
	for _, w := range engine.workers {
		w.Cancel()
	}

	engine.waitGroup.Wait()
}
