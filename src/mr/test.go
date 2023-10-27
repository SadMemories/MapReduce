package mr

import "fmt"
import "log"

// defer：在return的时候先给返回值赋值，然后调用defer函数，最后返回函数调用

func f() (result int) {
	defer func() {
		result++
	}()
	return 0
}

func f2() (r int) {
	t := 5
	defer func() {
		t = t + 5
	}()

	return t
}

func f3() (r int) {
	defer func(r int) {
		r = r + 5
	}(r)

	return 1
}

func main() {
	res := f()
	secR := f2()
	thrR := f3()
	fmt.Println(res)
	fmt.Println(secR)
	fmt.Println(thrR)

	log.Printf("%v\n", res)
}