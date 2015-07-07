package parallel

import (
	tk "github.com/eaciit/toolkit"
	"math"
	"sync"
	"time"
)

type JobResult struct {
	Status       string
	Count        int
	Run          int
	Success      int
	Fail         int
	FailIndexes  []int
	FailMessages []string
	Duration     time.Duration
	Output       interface{}
}

type T map[string]interface{}

func RunParallelJob(keys []interface{}, numberOfWorker int,
	f func(key interface{}, in T, result *JobResult) error, parms T) *JobResult {
	result := new(JobResult)
	result.Status = "OK"
	t0 := time.Now()
	result.FailIndexes = make([]int, 0)
	result.FailMessages = make([]string, 0)
	jobCount := len(keys)
	wg := new(sync.WaitGroup)
	wg.Add(jobCount)

	blockSize := int(math.Ceil(float64(jobCount) / float64(numberOfWorker)))
	for blockIdx := 0; blockIdx < numberOfWorker; blockIdx++ {
		go func(blockId int, w *sync.WaitGroup) {
			blockStartIdx := blockId * blockSize
			blockEndIdx := blockStartIdx + blockSize - 1
			if blockEndIdx > (jobCount - 1) {
				blockEndIdx = jobCount - 1
			}

			for keyIdx := blockStartIdx; keyIdx <= blockEndIdx; keyIdx++ {
				result.Run = result.Run + 1
				if f != nil {
					tk.Try(func() {
						key := keys[keyIdx]
						erun := f(key, parms, result)
						if erun != nil {
							result.Fail = result.Fail + 1
							result.FailIndexes = append(result.FailIndexes, keyIdx)
							result.FailMessages = append(result.FailMessages, erun.Error())
						} else {
							result.Success = result.Success + 1
						}
					}).Catch(func(e interface{}) {
						result.Fail = result.Fail + 1
						result.FailIndexes = append(result.FailIndexes, keyIdx)
						result.FailMessages = append(result.FailMessages, e.(error).Error())
					}).Finally(func() {
						wg.Done()
					}).Run()
				}
			}
		}(blockIdx, wg)
	}

	wg.Wait()
	result.Duration = time.Since(t0)
	return result
}
