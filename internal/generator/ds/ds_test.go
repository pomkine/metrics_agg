package ds

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNextVal_EqualInitValue(t *testing.T) {
	config := Config{
		ID:            "test",
		InitValue:     100,
		MaxChangeStep: 5,
	}
	sut := NewDataSource(config)

	actual := sut.GetData()
	assert.Equal(t, actual.Value, 100)
}

func TestNextVal_IncreasedUpToMaxChangeStep(t *testing.T) {
	config := Config{
		ID:            "test",
		InitValue:     100,
		MaxChangeStep: 5,
	}
	sut := NewDataSource(config)

	_ = sut.GetData()
	actual := sut.GetData()

	assert.Greater(t, actual.Value, 100)
	assert.LessOrEqual(t, actual.Value, 100+10)
}
