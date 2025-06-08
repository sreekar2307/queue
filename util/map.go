package util

func Keys[T any, K comparable](m map[K]T) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func Values[T any, K comparable](m map[K]T) []T {
	values := make([]T, 0, len(m))
	for _, v := range m {
		values = append(values, v)
	}
	return values
}
