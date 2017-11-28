package runner

import (
	"fmt"
	"log"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func jobHandleFunc(errRate float64, jobTime time.Duration) func(job Request) (Response, error) {
	rand.Seed(time.Now().UTC().UnixNano())

	return func(job Request) (Response, error) {
		begin := time.Now()
		res := Response{}
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

func TestShort(t *testing.T) {
	rates := []float64{1, 20}
	bursts := []int{1, 4096}
	runTimes := []time.Duration{2 * time.Second, 10 * time.Second}
	errorRates := []float64{0, 0.2}
	jobMaxExecTimes := []time.Duration{1 * time.Millisecond, 1 * time.Second}
	runTests(t, rates, bursts, runTimes, errorRates, jobMaxExecTimes)
}

func TestLong(t *testing.T) {

	rates := []float64{1, 20, 300, 4000, 10000}
	bursts := []int{1, 4096}
	runTimes := []time.Duration{2 * time.Second, 10 * time.Second, 60 * time.Second}
	errorRates := []float64{0, 0.3}
	jobMaxExecTimes := []time.Duration{0, 10 * time.Millisecond, 100 * time.Millisecond, 1 * time.Second}

	runTests(t, rates, bursts, runTimes, errorRates, jobMaxExecTimes)
}

func runTests(t *testing.T, rates []float64, bursts []int, runTimes []time.Duration, errorRates []float64, jobMaxExecTimes []time.Duration) {

	rampDown := 500 * time.Millisecond
	statInterval := 2 * time.Second
	tt := []testCase{}

	for _, d := range runTimes {
		for _, burst := range bursts {
			for _, errRate := range errorRates {
				for _, rate := range rates {
					for _, jobTime := range jobMaxExecTimes {
						tc :=
							testCase{
								name:            fmt.Sprintf("rate_%v,\tburst_%v,\trunTime %v,\terrRate %v,\tjobTime %v", rate, burst, d, errRate, jobTime),
								burst:           burst,
								rate:            rate,
								runPeriod:       d,
								rampDown:        rampDown,
								errRate:         errRate,
								statInterval:    statInterval,
								jobTime:         jobTime,
								expectedStatCnt: int(d / statInterval),
								expectedRunTime: d,
								expectedOkCnt:   int64((float64(d / time.Second)) * rate * (1.0 - errRate)),
								expectedErrCnt:  int64((float64(d / time.Second)) * rate * errRate),
								expectedOkRate:  rate * (1.0 - errRate),
								expectedErrRate: rate * errRate,
							}
						tt = append(tt, tc)
					}
				}
			}
		}
	}

	for _, tc := range tt {

		t.Run(tc.name, func(t *testing.T) {
			statChan := make(chan Stat)
			go Run(tc.runPeriod, tc.rampDown, tc.rate, tc.burst, tc.statInterval, statChan, jobHandleFunc(tc.errRate, tc.jobTime))

			okCntTotal, errCntTotal := int64(0), int64(0)
			startTime := time.Now()

			statCnt := 0
			for s := range statChan {
				okCntTotal += s.OkCnt
				errCntTotal += s.ErrCnt
				interval := s.IntervalSize
				okRate := s.OkCnt * int64(time.Second) / int64(interval)
				errRate := s.ErrCnt * int64(time.Second) / int64(interval)

				statCnt++
				if statCnt >= tc.expectedStatCnt { //last stat, ignore
					break
				}
				if okRate != 0 && tc.expectedOkRate > 0 {
					assert.InEpsilon(t, tc.expectedOkRate, okRate, 0.5, "%+v %+v okRate expected %v actual %v", tc, s, tc.expectedOkRate, okRate)
					// log.Printf("%+v %+v okRate expected %v actual %v", tc, s, tc.expectedOkRate, okRate)
				}
				if errRate != 0 && tc.expectedErrRate > 0 {
					assert.InEpsilon(t, tc.expectedErrRate, errRate, 0.5, "%+v %+v errRate expected %v actual %v", tc, s, tc.expectedErrRate, errRate)
					// log.Printf("%+v %+v errRate expected %v actual %v", tc, s, tc.expectedErrRate, errRate)
				}

				avgDoTime := time.Duration(0)
				if s.OkCnt != 0 {
					avgDoTime = s.SumDoTime / time.Duration(s.OkCnt)
				}
				if avgDoTime != 0 {
					assert.InEpsilon(t, tc.jobTime, avgDoTime, 0.5, "%+v %+v avgDoTime expected %v actual %v", tc, s, tc.jobTime, avgDoTime)
				}
			}

			assert.InDelta(t, tc.expectedStatCnt, statCnt, 1, "case %+v\t: expected stat cnt %v,actual %v", tc, tc.expectedStatCnt, statCnt)

			elapsed := time.Since(startTime)

			log.Printf("case %+v\n:Elapsed time %v,\tdone %v,\terrors %v", tc, elapsed, okCntTotal, errCntTotal)
			delta := tc.rampDown
			if tc.jobTime > delta {
				delta = tc.jobTime
			}
			assert.InDelta(t, int64(tc.expectedRunTime), int64(elapsed), float64(delta+500*time.Millisecond), "case %+v\t:Elapsed time %v,\tdone %v,\terrors %v", tc, elapsed, okCntTotal, errCntTotal)

			if tc.jobTime > tc.rampDown {
				return
			}

			//when jobTime is greater than ramp downTime, job results discarded
			//we expected all job to be handled unless
			if okCntTotal != 0 && tc.expectedOkCnt > 10 {
				assert.InEpsilon(t, tc.expectedOkCnt, okCntTotal, 0.2, "case %+v OKcnt expected %v actual %v", tc, tc.expectedOkCnt, okCntTotal)
			}
			if errCntTotal != 0 && tc.expectedErrCnt > 10 {
				assert.InEpsilon(t, tc.expectedErrCnt, errCntTotal, 0.2, "case %+v ErrCnt expected %v actual %v", tc, tc.expectedErrCnt, errCntTotal)
			}
		})
	}

}
