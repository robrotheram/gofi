package main

import "fmt"
import "reflect"

type Something interface {
	SetX(x int)
}

type RealThing struct {
	x int
}

func (t *RealThing) SetX(x int) {
	t.x = x
}

func Updated(original Something, newX int) Something {
	val := reflect.ValueOf(original)
	if val.Kind() == reflect.Ptr {
		val = reflect.Indirect(val)
	}
	newThing := reflect.New(val.Type()).Interface().(Something)
	newThing.SetX(newX)
	return newThing
}

func main() {
	a := &RealThing{x: 1}
	b := Updated(a, 5)
	fmt.Printf("a = %v\n", a)
	fmt.Printf("b = %v\n", b)
}
