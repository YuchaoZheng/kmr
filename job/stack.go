package job

type stack []int

func (s stack) Push(v int) stack {
	return append(s, v)
}

func (s stack) Pop() (stack, int) {
	// FIXME: What do we do if the stack is empty, though?

	l := len(s)
	return  s[:l-1], s[l-1]
}