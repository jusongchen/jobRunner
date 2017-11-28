package runner

import (
	"context"
	"fmt"
	"os"
	"runtime/debug"
	"sync"
	"time"
)

//Stat basic counters
type Stat struct {
	IntervalNum  int
	OkCnt        int64
	ErrCnt       int64
	MinDoTime    time.Duration
	MaxDoTime    time.Duration
	SumDoTime    time.Duration
	IntervalSize time.Duration
	LastUdpate   time.Time
}

//Request describes a job
type Request struct{ JobID int64 }

//Response describes process result
type Response struct {
	Elapsed time.Duration
	Msg     string
}

//worker process the job request
//After job is done, it sends result to resChan. it will discard results if  distcardResult signal received.
func worker(discardResult <-chan struct{}, wg *sync.WaitGroup, req Request, resChan chan<- Response, errChan chan<- error, handleFunc func(job Request) (Response, error)) {

	defer func() {
		if rvr := recover(); rvr != nil {
			fmt.Fprintf(os.Stderr, "Panic: %+v\n", rvr)
			debug.PrintStack()
		}
		wg.Done()
	}()

	start := time.Now()
	res, err := handleFunc(req)
	res.Elapsed = time.Since(start)
	select {

	case <-discardResult: //controller ask to discardResult as resChan and errChan are to be closed
		fmt.Fprintf(os.Stderr, "Result discard: %+v, err: %v\n", res, err)
		return
	default:
		if err != nil {
			//if errChan closed , deferred recover() will save us
			errChan <- err
		} else {
			//if resChan closed , deferred recover() will save us
			resChan <- res
		}
	}

}

// WaitTimeout returns when either 1) wg *sync.WaitGroup is donw 2) graceTime passed after the ctx.done() signal
func WaitTimeout(ctx context.Context, graceTime time.Duration, wg *sync.WaitGroup) {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	select {
	case <-ch:
		return
	case <-ctx.Done():
		//cancel signal detected, but the waitgroup not done yet. wait a graceful period
		<-time.After(graceTime)
		return
	}
}

//processJobs:
//  invoke workers at rate to do jobs
//	close resChan and errChan when returns
func processJobs(ctx context.Context, requestPerSec float64, runPeriod, rampDown time.Duration, resChan chan<- Response, errChan chan<- error, handleFunc func(job Request) (Response, error)) {
	// var flagQuit uint64

	// use a WaitGroup
	var wg sync.WaitGroup

	//to signal if all processed results should be discarded
	discardResult := make(chan struct{})

	//by default , we send 1 request for each cycle
	// for 10 request/second, we do 10 cycles
	timePerCycle := time.Second / time.Duration(requestPerSec)
	requestPerCycle := 1

	//when requested rate is high, we send multpile requests in one cycle
	//time.tick() wont give us precise control when cycle is less than 1 ms.
	for timePerCycle < 20*time.Millisecond { // chosen arbitrarily
		requestPerCycle *= 2
		timePerCycle *= 2
	}

	throttle := time.Tick(timePerCycle)

	//run period
	ctxRun, cancel := context.WithTimeout(ctx, runPeriod)
	defer cancel()

	newJob := newReqFunc()

	//calculate max request # at this rate.
	req2Send := int64((float64(runPeriod) / float64(time.Second)) * requestPerSec)

ForLoop:
	for {
		select {
		case <-ctxRun.Done(): //rampup and steady Period timeout
			break ForLoop
		default:

			for i := 0; i < requestPerCycle; i++ {
				req := newJob()
				wg.Add(1)
				go worker(discardResult, &wg, req, resChan, errChan, handleFunc)
				req2Send--
				if req2Send == 0 {
					break ForLoop
				}
			}
			<-throttle //wait next tick
		}
	}

	//WaitTimeout returns when
	//1) all waitGroup are done, or
	//2) rampDown period has passed after ctx.done() signal
	WaitTimeout(ctx, rampDown, &wg)

	//send signal to ask worker to abandon any processed results as we are closing result channels
	close(discardResult)

	close(resChan)
	close(errChan)

}

func newReqFunc() func() Request {
	jobID := int64(0)
	return func() Request {
		r := Request{
			JobID: jobID,
		}
		jobID++
		return r
	}
}

//if eof, then both resChan and errChan are drained to empty
func calcStat(interval time.Duration, resChan <-chan Response, errChan <-chan error) (s Stat, eof bool) {

	beginTime := time.Now()
	s = Stat{}

	/*	read from two channels
		end for loop when done signal received
		or all channels drained
	*/
	timeout := time.After(interval)
forLoop:
	for {
		select {
		case <-timeout: //stop processing when time out
			break forLoop
		default:
		}

		select {
		case res, ok := <-resChan:
			if !ok { //resChan closed
				resChan = nil // block read from resChan
			} else {
				s.OkCnt++
				s.SumDoTime += res.Elapsed
				if res.Elapsed > s.MaxDoTime {
					s.MaxDoTime = res.Elapsed
				}
				if res.Elapsed < s.MinDoTime || s.MinDoTime == 0 {
					s.MinDoTime = res.Elapsed
				}
			}

		case err, ok := <-errChan:
			if !ok { //errChan closed
				errChan = nil // block read from errChan
			} else {
				s.ErrCnt++
				_ = err
			}
		default: //all msg processed
			if errChan == nil && resChan == nil {
				eof = true
				<-timeout //wait for wait out
				break forLoop
			}
		}
	}

	s.LastUdpate = time.Now()
	s.IntervalSize = s.LastUdpate.Sub(beginTime)

	return s, eof
}

//Run process requests at a fixed rate for runPeriod
func Run(runPeriod, waitToComplete time.Duration, rate float64, burst int, statInterval time.Duration, statChan chan<- Stat, handleFunc func(job Request) (Response, error)) {

	resChan := make(chan Response, burst)
	errChan := make(chan error, burst)

	totalDuration := runPeriod + waitToComplete
	ctx, cancel := context.WithTimeout(context.Background(), totalDuration)
	defer cancel()

	defer close(statChan)

	go processJobs(ctx, rate, runPeriod, waitToComplete, resChan, errChan, handleFunc)

	for statNum := 0; ; statNum++ {
		s, eof := calcStat(statInterval, resChan, errChan)
		s.IntervalNum = statNum
		select {
		case statChan <- s:
		default: //cannot send stat to statChan, discard it
			fmt.Fprintf(os.Stderr, "stat sent not accepted, distcard stat:%v\n", s)
		}
		if eof { //both errChan and resChan drained
			break
		}
	}
}
