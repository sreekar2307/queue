package main

func UniqBy[T any, K comparable](slice []T, keyFunc func(T) K) []T {
	seen := make(map[K]bool)
	var result []T
	for _, item := range slice {
		key := keyFunc(item)
		if !seen[key] {
			seen[key] = true
			result = append(result, item)
		}
	}
	return result
}

func Map[T any, E any](slice []T, mapFunc func(T) E) []E {
	var result []E
	for _, item := range slice {
		result = append(result, mapFunc(item))
	}
	return result
}
