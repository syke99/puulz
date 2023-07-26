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

func TestNewPuul(t *testing.T) {
	// Arrange
	dataStore := []data{{fail: false, greeting: "english"}, {fail: true, greeting: ""}, {fail: false, greeting: "spanish"}, {fail: true, greeting: ""}, {fail: false, greeting: "mandarin"}, {fail: false, greeting: "french"}}

	// Act
	_, err := NewPuul[data, map[string]string](2, dataStore, worker)

	// Assert
	assert.NoError(t, err)
}

func TestNewPuul_Error(t *testing.T) {
	// Arrange
	dataStore := []data{{fail: false, greeting: "english"}, {fail: true, greeting: ""}, {fail: false, greeting: "spanish"}, {fail: true, greeting: ""}, {fail: false, greeting: "mandarin"}, {fail: false, greeting: "french"}}

	// Act
	_, err := NewPuul[data, map[string]string](7, dataStore, worker)

	// Assert
	assert.Error(t, err)
	assert.Equal(t, errPuulSize, err)
}

func TestWithErrorChannel(t *testing.T) {
	// Arrange
	dataStore := []data{{fail: false, greeting: "english"}, {fail: true, greeting: ""}, {fail: false, greeting: "spanish"}, {fail: true, greeting: ""}, {fail: false, greeting: "mandarin"}, {fail: false, greeting: "french"}}

	pool, _ := NewPuul[data, map[string]string](2, dataStore, worker)

	// Act
	_ = pool.WithErrorChannel()

	// Assert
	assert.NotNil(t, pool.errChan)
}

func TestPuulRun(t *testing.T) {
	// Arrange
	dataStore := []data{{fail: false, greeting: "english"}, {fail: true, greeting: ""}, {fail: false, greeting: "spanish"}, {fail: true, greeting: ""}, {fail: false, greeting: "mandarin"}, {fail: false, greeting: "french"}}

	pool, _ := NewPuul[data, map[string]string](2, dataStore, worker)

	errChan := pool.WithErrorChannel()

	// Act
	pool.Run([]map[string]string{greetings})

	for err := range errChan {
		assert.Regexp(t, regexp.MustCompile("\"index\": (1|3), \"error_msg\": \"this worker failed\""), err)
	}
}

func TestPuulRun_WithAutoRefill(t *testing.T) {
	// Arrange
	dataStore := []data{{fail: false, greeting: "english"}, {fail: true, greeting: ""}, {fail: false, greeting: "spanish"}, {fail: true, greeting: ""}, {fail: false, greeting: "mandarin"}, {fail: false, greeting: "french"}}

	pool, _ := NewPuul[data, map[string]string](2, dataStore, worker)

	errChan := pool.WithErrorChannel()

	pool.WithAutoRefill()

	// Act
	pool.Run([]map[string]string{greetings})

	for err := range errChan {
		assert.Regexp(t, regexp.MustCompile("\"index\": (1|3), \"error_msg\": \"this worker failed\""), err)
	}
}
