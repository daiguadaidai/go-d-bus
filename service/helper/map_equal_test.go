package helper

import (
	"fmt"
	"testing"
)

func TestMapAGreaterOrEqualMapB(t *testing.T) {
	mapA := map[string]interface{}{
		"id":   110,
		"name": "HH",
	}
	mapB := map[string]interface{}{
		"id":   111,
		"name": "HH",
	}

	fmt.Printf("%v >= %v: %v", mapA, mapB, MapAGreaterOrEqualMapB(mapA, mapB))
}
