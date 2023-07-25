package puulz

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

var greetings = map[string]string{
	"english": "hello",
	"spanish": "hola",
}

type data struct {
	fail     bool
	greeting string
}

func worker(d data, params []any) error {
	if d.fail {
		return errors.New("this worker failed")
	}

	greetings := params[0].(map[string]string)

	fmt.Println(greetings[d.greeting])
	return nil
}

func TestPuulRun(t *testing.T) {
	// Arrange
	dataStore := []data{{fail: false, greeting: "english"}, {fail: true, greeting: ""}, {fail: false, greeting: "spanish"}}

	errChan := make(chan error, len(dataStore))

	pool, _ := NewPuul[data, any](2, dataStore, worker, errChan)

	pool.WithAutoRefill()

	// Run
	pool.Run([]any{greetings})

	for err := range errChan {
		assert.Equal(t, errors.New("data at index: 1, error msg: this worker failed"), err)
	}
}
