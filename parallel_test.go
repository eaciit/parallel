package parallel

import (
	"fmt"
	"github.com/eaciit/database/base"
	"github.com/eaciit/database/mongodb"
	"github.com/eaciit/toolkit"
	"runtime"
	"strconv"
	"testing"
	"time"
)

var e error

const (
	pworkerCount = 100
	pnumCount    = 20000
)

func init() {
	fmt.Println("Initiate Env")
	runtime.GOMAXPROCS(runtime.NumCPU())
	fmt.Println("")
}

func Test1(t *testing.T) {
	t.Skip()
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
	pr := Run(idxs, workerCount, func(js <-chan interface{}, rs chan<- *toolkit.Result) {
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

func TestDb1(t *testing.T) {
	//t.Skip()
	t0 := time.Now()
	fmt.Println("Test 2a - Insert Data (No Parallel)")
	numCount := pnumCount
	idxs := make([]interface{}, numCount)
	for i := 1; i <= numCount; i++ {
		idxs[i-1] = i
	}
	conn := mongodb.NewConnection("localhost:27888", "root", "Database.1", "ectest")
	//conn := mongodb.NewConnection("localhost:27888", "", "", "ectest")
	e = conn.Connect()
	if e != nil {
		t.Error("Unable to connect to Database | " + e.Error())
	}
	defer conn.Close()
	conn.Execute("appusers", toolkit.M{"find": toolkit.M{}, "operation": base.DB_DELETE})

	success := 0
	fmt.Printf("Insert %d data with no pool \n", numCount)
	for _, jb := range idxs {
		j := jb.(int)
		user := toolkit.M{}
		userid := "User " + strconv.Itoa(j)
		user.Set("_id", userid)
		user.Set("fullname", "User "+strconv.Itoa(j))
		user.Set("email", "user"+strconv.Itoa(j)+"@email.com")
		//_, _, e = conn.Query().From("ORMUsers").Save(user).Run()
		_, e := conn.Execute("appusers", toolkit.M{"find": toolkit.M{"_id": userid}, "operation": base.DB_SAVE, "data": user})
		if e == nil {
			success++
		}
	}

	if success == numCount {
		fmt.Printf("Test OK in %v \n\n", time.Since(t0))
	} else {
		fmt.Printf("Test Fail has %d records in %v \n\n", success, time.Since(t0))
	}
}

func TestDb2(t *testing.T) {
	fmt.Println("Test 2b - Insert Data (Parallel)")
	numCount := pnumCount
	workerCount := pworkerCount
	idxs := make([]interface{}, numCount)
	for i := 1; i <= numCount; i++ {
		idxs[i-1] = i + 2000
	}

	conn := mongodb.NewConnection("localhost:27888", "root", "Database.1", "ectest")
	//conn := mongodb.NewConnection("localhost:27888", "", "", "ectest")
	e = conn.Connect()
	if e != nil {
		t.Error("Unable to connect to Database | " + e.Error())
		return
	}
	defer conn.Close()
	_, e = conn.Execute("appusers", toolkit.M{"find": toolkit.M{}, "operation": base.DB_DELETE})
	if e != nil {
		fmt.Println("Unable to delete: " + e.Error())
	}

	fmt.Printf("Insert %d data within %d pools \n", numCount, workerCount)
	pr := Run(idxs, workerCount, func(js <-chan interface{}, rs chan<- *toolkit.Result) {
		for jb := range js {
			j := jb.(int)
			user := toolkit.M{}
			userid := "User " + strconv.Itoa(j)
			user.Set("_id", userid)
			user.Set("fullname", "User "+strconv.Itoa(j))
			user.Set("email", "user"+strconv.Itoa(j)+"@email.com")
			r := new(toolkit.Result)
			//_, _, e = conn.Query().From("ORMUsers").Save(user).Run()
			_, e := conn.Execute("appusers",
				toolkit.M{"find": toolkit.M{"_id": userid}, "operation": base.DB_SAVE, "data": user})
			if e == nil {
				r.Status = toolkit.Status_OK
				r.Data = user
			} else {
				r.Status = toolkit.Status_NOK
				r.Message = "Error ID " + strconv.Itoa(j) + " " + e.Error()
			}
			rs <- r
		}
	})

	if pr.Success == numCount {
		fmt.Printf("Test OK in %v \n\n", pr.Duration)
	} else {
		fmt.Printf("Test Fail has %d records. \nErrors\n%v\nIn %v \n\n", pr.Success, toolkit.JsonString(pr.Errors[0]), pr.Duration)
	}
}
