package main

import "fmt"
import "sync"

// 编写一个程序限制10个goroutine执行，每执行完一个goroutine就放一个新的goroutine进来
func main() {
	done := make(chan struct{}, 10)
	var wg sync.WaitGroup
	
	for i := 0; i < 20; i ++ {
		wg.Add(1)
		done <- struct{}{}
		go func(taskid int) {
			defer wg.Done()
			dotask(taskid)
			<- done
		}(i)
	}
	
	wg.Wait()
}

func dotask(x int) {
	fmt.Println(x)
}