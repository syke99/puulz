package puulz

import (
	"errors"
	"fmt"
	"sync/atomic"
)

type Puul[T any] struct {
	size       int
	datastore  []T
	batches    [][]T
	batchCount int
	worker     func(data T) error
	errChan    chan error
	autoRefil  bool
}

func NewPuul[T any](size int, dataStore []T, worker func(data T) error, errChan chan error) (*Puul[T], error) {
	if len(dataStore) < size {
		return nil, errors.New("length of dataStore much be greater than or equal to size")
	}

	// calculate number of batches
	batched := len(dataStore) / size

	bMod := len(dataStore) % size

	if bMod != 0 {
		batched++
	}

	// divide dataStore into batches
	total := 0

	batches := make([][]T, batched)

	for total < batched {
		offset := (size * total)

		batches[total] = dataStore[(size * total) : offset+(size-1)]
		total++
	}

	p := Puul[T]{
		size:       size,
		datastore:  dataStore,
		batches:    batches,
		batchCount: 1,
		worker:     worker,
		errChan:    errChan,
		autoRefil:  false,
	}

	return &p, nil
}

func (p *Puul[T]) WithAutoRefill() *Puul[T] {
	p.autoRefil = true
	return p
}

func (p *Puul[T]) Run() {
	i := 0

	var finished int32

	workerDone := make(chan struct{})

	// spin up initial goroutines
	for i < p.size {
		go work[T](p.datastore[i], int32(len(p.datastore)), p.worker, p.errChan, i, workerDone, &finished)
		i++
	}

	if p.autoRefil {
		for range workerDone {
			// count how many worker funcs have finished
			idx := int(atomic.LoadInt32(&finished))
			// if the initial size hasn't been reached, meaning the initial
			// batch of workers hasn't completed, continue on to the next
			// message in workerDone
			if idx < p.size {
				continue
			}

			// since p.autoRefill is true, continue to refill the Puul with
			// worker funcs and a data source as each worker func finished
			// until all data sources have been depleted
			if idx >= p.size && idx < len(p.datastore) {
				go work[T](p.datastore[idx], int32(len(p.datastore)), p.worker, p.errChan, idx, workerDone, &finished)
			}
		}
	} else {
		for range workerDone {
			// count how many worker funcs have finished
			idx := int(atomic.LoadInt32(&finished))
			// if the initial size hasn't been reached, meaning the initial
			// batch of workers hasn't completed, continue on to the next
			// message in workerDone
			if idx < p.size {
				continue
			}

			mod := idx % p.size

			// if mod == 0, that means the batch has completed and it's
			// time to move on to the next batch
			if mod == 0 {
				// increase the batch count
				p.batchCount++

				// if there's more batches to go, carry on
				if p.batchCount <= len(p.batches) {
					x := 0

					// get the right batch (p.batchCount starts at 1,
					// so subtract 1 to get the correct batch)
					dS := p.batches[p.batchCount-1]

					// loop through the batch and spin up goroutines
					for x < len(dS) {
						dataIndex := len(p.batches[p.batchCount-1]) + x

						go work[T](dS[x], int32(len(p.datastore)), p.worker, p.errChan, dataIndex, workerDone, &finished)
					}
				}
			}
		}
	}
}

func work[T any](data T, dataLength int32, worker func(data T) error, errChan chan error, idx int, workerDone chan struct{}, finished *int32) {
	// execute worker on data
	err := worker(data)

	// wrap and send error through errChan if worker errors
	if err != nil {
		errChan <- fmt.Errorf("data at index: %d, error msg: %s", idx, err.Error())
	}

	// once worker finishes, increment number of workers finished
	atomic.AddInt32(finished, 1)

	workerDone <- struct{}{}

	// if the number of finished workers is equal to the total
	// number of data sources to execute a worker for, then
	// close the workerDone chan
	if atomic.LoadInt32(finished) == dataLength {
		close(workerDone)
	}
}
