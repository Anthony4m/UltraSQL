package utils

type Iterator[T any] interface {
	HasNext() bool
	Next() T
}

type SliceIterator[T any] struct {
	slice []T
	index int
}

func (it *SliceIterator[T]) HasNext() bool {
	return it.index < len(it.slice)
}

func (it *SliceIterator[T]) Next() T {
	value := it.slice[it.index]
	it.index++
	return value
}
