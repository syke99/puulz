package puulz

import (
	"errors"
	"fmt"
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
)

var greetings = map[string]string{
	"english":  "hello",
	"spanish":  "hola",
	"mandarin": "ni hao",
	"french":   "bonjour",
}

type data struct {
	fail     bool
	greeting string
}

func worker(d data, params []map[string]string) error {
	if d.fail {
		return errors.New("this worker failed")
	}

	grts := params[0]

	fmt.Println(grts[d.greeting])
	return nil
}

func TestPuulRun_WithAutoRefill(t *testing.T) {
	// Arrange
	dataStore := []data{{fail: false, greeting: "english"}, {fail: true, greeting: ""}, {fail: false, greeting: "spanish"}, {fail: true, greeting: ""}, {fail: false, greeting: "mandarin"}, {fail: false, greeting: "french"}}

	errChan := make(chan error, len(dataStore))

	pool, _ := NewPuul[data, map[string]string](2, dataStore, worker, errChan)

	pool.WithAutoRefill()

	// Run
	pool.Run([]map[string]string{greetings})

	for err := range errChan {
		assert.Regexp(t, regexp.MustCompile("data at index: (1|3), error msg: this worker failed"), err)
	}
}
