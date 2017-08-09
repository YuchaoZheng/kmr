package util

import "errors"

var EmptyStackError = errors.New("Stack is empty")

type Stack struct {
	items []interface{}
}

func (s Stack) Push(v interface{}) {
	s.items = append(s.items, v)
}

func (s Stack) Pop() error {
	if s.Size() == 0 {
		return EmptyStackError
	}
	s.items = s.items[:s.Size()-1]
	return nil
}

func (s Stack) Size() int {
	return len(s.items)
}

func (s Stack) Empty() bool {
	return s.Size() == 0
}

func (s Stack) Top() (interface{}, error) {
	if s.Size() == 0 {
		return nil, EmptyStackError
	}
	return s.items[s.Size()-1], nil
}

func (s Stack) Items() []interface{} {
	return s.items
}
