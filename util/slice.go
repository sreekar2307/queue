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
