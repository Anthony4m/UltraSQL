package utils

import "ultraSQL/kfile"

type Iterator[T any] interface {
	HasNext() bool
	Next() (*kfile.Cell, error)
}
