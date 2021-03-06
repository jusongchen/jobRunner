package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/jusongchen/jobRunner/pkg/runner"
)

func jobHandleFunc(errRate float64, jobTime time.Duration) func(job runner.Request) (runner.Response, error) {

	return func(job runner.Request) (runner.Response, error) {
		begin := time.Now()
		res := runner.Response{}
		defer func() {
			res.Elapsed = time.Since(begin)
		}()
		time.Sleep(jobTime)
		var err error

		f := rand.Float64()
		if f < errRate {
			err = fmt.Errorf("handling job %v \tError out", job.JobID)
			// fmt.Printf("%v\r", err)

		} else {
			res.Msg = fmt.Sprintf("handling job %v\tOK", job.JobID)
			// fmt.Printf("%v\r", res)
		}
		return res, err
	}
}

type testCase struct {
	name         string
	burst        int
	rate         float64
	runPeriod    time.Duration
	rampDown     time.Duration
	errRate      float64
	statInterval time.Duration

	jobTime time.Duration

	expectedStatCnt int
	expectedRunTime time.Duration
	expectedOkCnt   int64
	expectedErrCnt  int64
	expectedOkRate  float64
	expectedErrRate float64
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	tc := testCase{
		name:         "main",
		burst:        1e4,
		rate:         20,
		runPeriod:    7 * time.Second,
		rampDown:     time.Second,
		errRate:      0,
		statInterval: 2 * time.Second,
		jobTime:      time.Millisecond,
	}

	statChan := make(chan runner.Stat)
	ctx := context.Background()
	l := log.New(os.Stderr, "", log.LstdFlags)

	go runner.Run(ctx, l, tc.runPeriod, tc.rampDown, tc.rate, tc.burst, tc.statInterval, statChan, jobHandleFunc(tc.errRate, tc.jobTime))

	okCntTotal, errCntTotal := int64(0), int64(0)
	// startTime := time.Now()

	fmt.Printf("%#v\n", tc)
	// statCnt := 0
	for s := range statChan {
		okCntTotal += s.OkCnt
		errCntTotal += s.ErrCnt
		// okRate := s.OkCnt * int64(time.Second) / int64(interval)
		// errRate := s.ErrCnt * int64(time.Second) / int64(interval)
		fmt.Printf("%#v\n", s)
	}
	fmt.Printf("Total %v , Ok %v, Err %v", okCntTotal+errCntTotal, okCntTotal, errCntTotal)

}
