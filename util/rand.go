package util

import "math/rand"

// Sample randomly selects n elements from a slice. elements selected are unique by index
func Sample[T any](slice []T, n int) []T {
	if n > len(slice) {
		n = len(slice)
	}
	if n <= 0 {
		return nil
	}

	// Create a copy of the original slice to avoid modifying it
	sliceCopy := make([]T, len(slice))
	copy(sliceCopy, slice)

	// Shuffle the copied slice
	rand.Shuffle(len(sliceCopy), func(i, j int) {
		sliceCopy[i], sliceCopy[j] = sliceCopy[j], sliceCopy[i]
	})

	// Return the first n elements from the shuffled slice
	return sliceCopy[:n]
}
