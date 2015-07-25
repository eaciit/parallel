package parallel

import (
	"fmt"
	"github.com/eaciit/toolkit"
	"testing"
	"time"
)

var e error

func Test1(t *testing.T) {
	fmt.Println("Test 1 - Generate x Number")
	numCount := 1000
	workerCount := 100
	idxs := make([]interface{}, numCount)
	want := 0
	for i := 1; i <= numCount; i++ {
		idxs[i-1] = i
		want += i
	}

	total := 0
	fmt.Printf("Run Parallel %d job within %d pools \n", numCount, workerCount)
	pr := RunParallel(idxs, workerCount, func(js <-chan interface{}, rs chan<- *toolkit.Result) {
		for j := range js {
			time.Sleep(1 * time.Microsecond)
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
