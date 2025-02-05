package transaction

import "ultraSQL/buffer"

type BufferList struct {
}

func NewBufferList(*buffer.BufferMgr) *BufferList {
	return &BufferList{}
}
