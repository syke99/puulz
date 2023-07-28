package puulz

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Puul holds datastore of data to be processed and the worker func to be processed for each
// piece of data in the datastore, and represents a generic worker pool
type Puul[D, P any] struct {
	size        int
	datastore   []D
	batches     [][]D
	batchCount  int
	worker      func(data D, params []P) error
	errChan     chan error
	autoRefil   bool
	cancel      context.CancelFunc
	ctx         context.Context
	withCancel  bool
	withTimeout bool
}

// ErrPuulSize is the error returned if the length of the dataStore is less than the size
// whenever calling NewPuul()
var ErrPuulSize = errors.New("length of dataStore much be greater than or equal to size")

// NewPuul takes in a size to limit the amount of worker funcs to be running concurrently at once,
// a generic datastore (if the worker funcs need to opperate on some data), and a worker func with
// the function signature of func(data D, params []P) error. NewPuul (as well as the returned Puul
// and all methods implemented on it) is generic where D is the datastore and P are params to be
// passed to worker funcs whenever calling (*Puul[D, P]).Run(). Whenever creating a new Puul, if
// no specific data needs to be processed, but instead, the worker funcs will have their own
// functionality independent of data, then create a slice of empty structs to be passed to determine
// how many worker funcs will be ran in total
func NewPuul[D, P any](size int, dataStore []D, worker func(data D, params []P) error) (*Puul[D, P], error) {
	if len(dataStore) < size {
		return nil, ErrPuulSize
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

// WithErrorChannel returns a buffered chan error to collect any errors returned from
// worker funcs. (*Puul[D, P]).Run() will handle closing this channel. Simply range
// over the channel after calling (*Puul[D, P]).Run() to collect any errors
func (p *Puul[D, P]) WithErrorChannel() chan error {
	// p.errChan = make(chan error)
	p.errChan = make(chan error, len(p.datastore))
	return p.errChan
}

// WithAutoRefill will automatically refill the Puul with a new worker func (if any
// data items are left in the datastore to process) once one worker func finishes.
// Puuls will default to running in a batched state, so this method must be called
// before calling (*Puul[D, P]).Run() to toggle this behavior
func (p *Puul[D, P]) WithAutoRefill() {
	p.autoRefil = true
}

// WithCancel is a wrapper around context.WithCancel(ctx context.Context)
// to make a Puul cancelable
func (p *Puul[D, P]) WithCancel(ctx context.Context) (context.Context, context.CancelFunc) {
	c, cancelFunc := context.WithCancel(ctx)

	p.ctx = c
	p.withCancel = true

	return c, cancelFunc
}

// WithTimeout is a wrapper around context.WithTimeout(ctx context.Context, duration time.Duration)
// to make a Puul timeout after the specified duration
func (p *Puul[D, P]) WithTimeout(ctx context.Context, duration time.Duration) context.Context {
	c, cancelFunc := context.WithTimeout(ctx, duration)

	p.ctx = c
	p.cancel = cancelFunc
	p.withTimeout = true

	return c
}

type fin struct {
	sync.RWMutex
	counter int
}

// Run accepts a slice of generic parameters to be passed to each worker func,
// then spins up goroutines up to the specified size, and then either processes
// remaining data sources in a batched behavior, or will replenish the Puul
// automatically if (*Puul[D, P]).WithAutoRefill() was called beforehand
func (p *Puul[D, P]) Run(workerParams []P) error {
	var err error

	if p.withCancel {
		err = p.runWithContext(workerParams)
	} else if p.withTimeout {
		defer p.cancel()
		err = p.runWithContext(workerParams)
	} else {
		p.run(workerParams)
	}

	return err
}

func (p *Puul[D, P]) runWithContext(workerParams []P) error {
	breakout := true

	var err error

	fire := make(chan struct{})

	fired := false

	for breakout {
		select {
		case <-p.ctx.Done():
			breakout = false
			err = p.ctx.Err()
			break
		case <-fire:
			close(fire)
			p.run(workerParams)
		default:
			if !fired {
				fired = true
				fire <- struct{}{}
			}
		}
	}

	return err
}

func (p *Puul[D, P]) run(workerParams []P) {
	i := 0

	var finished fin

	dataLength := len(p.datastore)

	workerDone := make(chan struct{}, dataLength)

	for i < p.size {
		go work[D, P](p.datastore[i], dataLength, workerParams, p.worker, p.errChan, i, workerDone)
		i++
	}

	if p.autoRefil {
		for {
			<-workerDone
			finished.Lock()
			finished.counter++
			idx := finished.counter
			finished.Unlock()

			if idx == dataLength {
				close(workerDone)
				break
			}

			if idx < p.size {
				continue
			}

			if idx >= p.size && idx < len(p.datastore) {
				go work[D, P](p.datastore[idx], dataLength, workerParams, p.worker, p.errChan, idx, workerDone)
			}
		}
	} else {
		for {
			<-workerDone
			finished.Lock()
			finished.counter++
			idx := finished.counter
			finished.Unlock()

			if idx == dataLength {
				close(workerDone)
				break
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
	if p.errChan != nil {
		close(p.errChan)
	}
}

func work[D, P any](data D, dataLength int, workerParams []P, worker func(data D, params []P) error, errChan chan error, idx int, workerDone chan struct{}) {
	err := worker(data, workerParams)
	if err != nil && errChan != nil {
		errChan <- fmt.Errorf("{\"index\": %d, \"error_msg\": \"%s\"}", idx, err.Error())
	}

	workerDone <- struct{}{}
}
