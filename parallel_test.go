package parallel

import (
	"fmt"
	"github.com/eaciit/toolkit"
	"testing"
)

func Test1(t *testing.T) {
	fmt.Println("Test 1 - Generate 1000 Number")
	idxs := make([]interface{}, 1000)
	want := 0
	for i := 1; i <= 1000; i++ {
		idxs[i-1] = i
		want += i
	}

	total := 0
	pr := RunParallel(idxs, 10, func(js <-chan interface{}, rs chan<- *toolkit.Result) {
		for j := range js {
			j2 := j.(int)
			total += j2
			r := new(toolkit.Result)
			r.Status = toolkit.Status_OK
			r.Data = j2
			rs <- r
		}
	})

	if total == want && pr.Success == 1000 {
		fmt.Printf("Test OK in %v \n\n", pr.Duration)
	} else {
		fmt.Printf("Test Fail want %d  got %d and has %d records\n", total, want, pr.Success)
	}
}
