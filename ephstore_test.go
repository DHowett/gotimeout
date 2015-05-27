package gotimeout

import (
	"fmt"
	"time"
)

func ExampleMap() {
	m := NewMap()

	m.Put("5sec", "value", 5*time.Second)
	m.Put("10sec", "value", 10*time.Second)

	time.Sleep(6 * time.Second)

	fmt.Println(m.Get("5sec"))
	fmt.Println(m.Get("10sec"))

	// Output: <nil> false
	// value true
}
