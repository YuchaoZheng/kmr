package util

import "errors"

type Stack struct {
	items []interface{}
}

func (s Stack) Push(v interface{}) {
	s.items = append(s.items, v)
}

func (s Stack) Pop() error {
	if s.Size() == 0 {
		return errors.New("Stack is empty")
	}
	s.items = s.items[:s.Size() - 1]
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
		return nil, errors.New("Stack is empty")
	}
	return s.items[s.Size() - 1], nil
}

func (s Stack) Items() []interface{} {
	return s.items
}
