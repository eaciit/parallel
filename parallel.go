package parallel

import (
	tk "github.com/eaciit/toolkit"
	"time"
)

type Result struct {
	Status   string
	Count    int
	Run      int
	Success  int
	Fail     int
	Errors   []string
	Duration time.Duration
	Data     []interface{}
}

func NewResult() *Result {
	return &Result{
		Errors: []string{},
		Data:   []interface{}{},
		Status: "",
	}
}

func Run(keys []interface{}, workercount int, f func(<-chan interface{}, chan<- *tk.Result)) *Result {
	t0 := time.Now()
	numOfJob := len(keys)
	jobKeys := make(chan interface{}, numOfJob)
	jobResults := make(chan *tk.Result, numOfJob)

	r := NewResult()
	r.Count = numOfJob
	r.Status = "Running"

	//--- pool the works
	for poolCount := 0; poolCount < workercount; poolCount++ {
		go f(jobKeys, jobResults)
	}

	//--- setting the key for work
	for keyId := 0; keyId < numOfJob; keyId++ {
		r.Run++
		jobKeys <- keys[keyId]
	}
	close(jobKeys)

	//--- collect the process
	for resultId := 0; resultId < numOfJob; resultId++ {
		jobResult := <-jobResults
		if jobResult.Status == tk.Status_OK {
			r.Success++
			r.Data = append(r.Data, jobResult.Data)
		} else {
			r.Fail++
			r.Errors = append(r.Errors, jobResult.Message)
		}
	}

	if r.Success == r.Count {
		r.Status = string(tk.Status_OK)
	} else {
		r.Status = string(tk.Status_NOK)
	}
	r.Duration = time.Since(t0)
	return r
}
