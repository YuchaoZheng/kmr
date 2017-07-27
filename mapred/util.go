package mapred

// ForEachValue iterate values using iterator
func ForEachValue(values ValueIterator, handler func(interface{})) {
	for v, err := values.Next(); err == nil; v, err = values.Next() {
		handler(v)
	}
}
