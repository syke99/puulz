package puulz

import (
	"errors"
	"fmt"
	"sync"
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

var errPuulSize = errors.New("length of dataStore much be greater than or equal to size")

func NewPuul[D, P any](size int, dataStore []D, worker func(data D, params []P) error) (*Puul[D, P], error) {
	if len(dataStore) < size {
		return nil, errPuulSize
	}

	dataLength := len(dataStore)

	batched := dataLength / size

	total := 0

	batches := make([][]D, batched)

	for total < batched {
		if total == 0 {
			batches[total] = dataStore[:size]
			total++
			continue
		}

		previous := size * total

		if total == batched-1 {
			batches[total] = dataStore[previous:]
			total++
			continue
		}

		offset := size * total

		batches[total] = dataStore[previous : previous+offset]
		total++
	}

	p := Puul[D, P]{
		size:       size,
		datastore:  dataStore,
		batches:    batches,
		batchCount: 1,
		worker:     worker,
		autoRefil:  false,
	}

	return &p, nil
}

func (p *Puul[D, P]) WithErrorChannel() chan error {
	p.errChan = make(chan error, len(p.datastore))
	return p.errChan
}

func (p *Puul[D, P]) WithAutoRefill() {
	p.autoRefil = true
}

type fin struct {
	sync.RWMutex
	counter int
}

func (p *Puul[D, P]) Run(workerParams []P) {
	i := 0

	var finished fin

	dataLength := len(p.datastore)

	workerDone := make(chan struct{}, dataLength)

	for i < p.size {
		go work[D, P](p.datastore[i], dataLength, workerParams, p.worker, p.errChan, i, workerDone)
		i++
	}

	if p.autoRefil {
	refillLoop:
		for {
			select {
			case <-workerDone:
				finished.Lock()
				finished.counter++
				idx := finished.counter
				finished.Unlock()

				if idx == dataLength {
					close(workerDone)
					break refillLoop
				}

				if idx < p.size {
					continue
				}

				if idx >= p.size && idx < len(p.datastore) {
					go work[D, P](p.datastore[idx], dataLength, workerParams, p.worker, p.errChan, idx, workerDone)
				}
			}
		}
	} else {
	batchedLoop:
		for {
			select {
			case <-workerDone:
				finished.Lock()
				finished.counter++
				idx := finished.counter
				finished.Unlock()

				if idx == dataLength {
					close(workerDone)
					break batchedLoop
				}

				if idx < p.size {
					continue
				}

				mod := idx % p.size

				if mod == 0 {
					p.batchCount++

					if p.batchCount <= len(p.batches) {
						x := 0

						dS := p.batches[p.batchCount-1]

						for x < len(dS) {
							dataIndex := len(p.batches[p.batchCount-1]) + x

							go work[D, P](dS[x], dataLength, workerParams, p.worker, p.errChan, dataIndex, workerDone)
							x++
						}
					}
				}
			}
		}
	}
	if p.errChan != nil {
		close(p.errChan)
	}
}

func work[D, P any](data D, dataLength int, workerParams []P, worker func(data D, params []P) error, errChan chan error, idx int, workerDone chan struct{}) {
	err := worker(data, workerParams)
	if err != nil && errChan != nil {
		errChan <- fmt.Errorf("{\"index\": %d, \"error_msg\": \"%s\"", idx, err.Error())
	}

	workerDone <- struct{}{}
}
