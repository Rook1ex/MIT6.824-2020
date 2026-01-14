package main

import (
	"fmt"
)

// 开启100个协程，顺序打印1-1000，且保证协程号1的，打印尾数为1的数字.. 以此类推

func main() {
	ch := make([]chan int, 100)

	for i := 0; i < 100; i ++ {
		ch[i] = make(chan int)
	}

	for i := 0; i < 100; i ++ {
		id := i + 1
		go func (id int, c chan int)  {
			for x := range c {
				fmt.Printf("Worker %d : %d\n", id, x)
			}
		}(id, ch[i])
	}

	for i := 1; i <= 1000; i ++ {
		id := i % 100
		
		if id == 0 {
			id = 100
		}

		ch[id - 1] <- i
	}

	for i := 0; i < 100; i ++ {
		close(ch[i])
	}
}