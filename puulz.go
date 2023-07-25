package puulz

import (
	"errors"
	"fmt"
	"sync/atomic"
)

type Puul[D, P any] struct {
	size       int
	datastore  []D
	batches    [][]D
	batchCount int
	worker     func(data D, params []P) error
	errChan    chan error
	autoRefil  bool
}

func NewPuul[D, P any](size int, dataStore []D, worker func(data D, params []P) error, errChan chan error) (*Puul[D, P], error) {
	if len(dataStore) < size {
		return nil, errors.New("length of dataStore much be greater than or equal to size")
	}

	dataLength := len(dataStore)

	// calculate number of batches
	batched := dataLength / size

	bMod := dataLength % size

	if bMod != 0 {
		batched++
	}

	// divide dataStore into batches
	total := 0

	batches := make([][]D, batched)

	for total < batched {
		previous := (size * total)

		if total == batched-1 {
			batches[total] = dataStore[previous:]
			continue
		}

		offset := (size * total)

		batches[total] = dataStore[previous : offset+(size-1)]
		total++
	}

	p := Puul[D, P]{
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

func (p *Puul[D, P]) WithAutoRefill() *Puul[D, P] {
	p.autoRefil = true
	return p
}

func (p *Puul[D, P]) Run(workerParams []P) {
	i := 0

	var finished int32

	workerDone := make(chan struct{})

	dataLength := int32(len(p.datastore))

	// spin up initial goroutines
	for i < p.size {
		go work[D, P](p.datastore[i], dataLength, workerParams, p.worker, p.errChan, i, workerDone, &finished)
		i++
	}

	if p.autoRefil {
		for range workerDone {
			// count how many worker funcs have finished
			idx := int(atomic.LoadInt32(&finished))
			// if the initial size hasn't been reached, meaning the initial
			// batch of workers hasn't completed, continue on to the next
			// message in workerDone
			if idx <= p.size {
				continue
			}

			// since p.autoRefill is true, continue to refill the Puul with
			// worker funcs and a data source as each worker func finished
			// until all data sources have been depleted
			if idx > p.size && idx < len(p.datastore) {
				go work[D, P](p.datastore[idx], dataLength, workerParams, p.worker, p.errChan, idx, workerDone, &finished)
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

						go work[D, P](dS[x], dataLength, workerParams, p.worker, p.errChan, dataIndex, workerDone, &finished)
					}
				}
			}
		}
	}
}

func work[D, P any](data D, dataLength int32, workerParams []P, worker func(data D, params []P) error, errChan chan error, idx int, workerDone chan struct{}, finished *int32) {
	// execute worker on data
	err := worker(data, workerParams)

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
