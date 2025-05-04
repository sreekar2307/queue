package util

func ToSet[T comparable](s []T) map[T]bool {
	set := make(map[T]bool)
	for _, s := range s {
		set[s] = true
	}
	return set
}

func FirstMatch[T any](slice []T, predicate func(T) bool) (T, bool) {
	for _, item := range slice {
		if predicate(item) {
			return item, true
		}
	}
	var zero T
	return zero, false
}

func Filter[T any](slice []T, predicate func(T) bool) []T {
	var result []T
	for _, item := range slice {
		if predicate(item) {
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

func GroupBy[T any, K comparable](slice []T, keyFunc func(T) K) map[K][]T {
	grouped := make(map[K][]T)
	for _, item := range slice {
		key := keyFunc(item)
		grouped[key] = append(grouped[key], item)
	}
	return grouped
}
