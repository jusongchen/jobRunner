package runner

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func jobHandleFunc(errRate float64, jobTime time.Duration) func(job Request) (Response, error) {

	return func(job Request) (Response, error) {
		timeout := time.After(jobTime)
		begin := time.Now()
		res := Response{}
		defer func() {
			res.Elapsed = time.Since(begin)
		}()
		var err error

		f := rand.Float64()
		if f < errRate {
			err = fmt.Errorf("handling job %v \tError out", job.JobID)
			// fmt.Printf("%v\r", err)

		} else {
			res.Msg = fmt.Sprintf("handling job %v\tOK", job.JobID)
			// fmt.Printf("%v\r", res)
		}
		<-timeout //wait job time

		return res, err
	}
}

type testCase struct {
	name         string
	burst        int
	rate         float64
	RunPeriod    time.Duration
	RampDown     time.Duration
	errRate      float64
	StatInterval time.Duration

	JobTime time.Duration

	expectedStatCnt        int
	ExpectedRunElapsedTime time.Duration
	expectedOkCnt          int64
	expectedErrCnt         int64
	expectedOkRate         float64
	expectedErrRate        float64
}

func TestAll(t *testing.T) {
	rates := []float64{1, 20, 300, 4000, 20000}
	bursts := []int{1, 1000, 10000}
	statInterval := 2 * time.Second
	runTimes := []time.Duration{4 * time.Second, 11 * time.Second}
	errorRates := []float64{0.0, 0.2}
	jobMaxExecTimes := []time.Duration{0, 10 * time.Millisecond, 1 * time.Second}

	rampDown := 500 * time.Millisecond
	tt := []testCase{}
	rand.Seed(time.Now().UTC().UnixNano())

	for _, d := range runTimes {
		for _, burst := range bursts {
			for _, errRate := range errorRates {
				for _, rate := range rates {
					for _, jobTime := range jobMaxExecTimes {
						expectedStatcnt := int(d / statInterval)
						if expectedStatcnt == 0 {
							expectedStatcnt++
						}
						if int(rate)/burst > 400 {
							continue
						}

						if burst > 1000 && rate < 1000 {
							continue
						}
						expectedStatCnt := int(math.Ceil(float64(d) / float64(statInterval)))
						expectedTotalReq := int64(math.Floor(0.5 + (float64(d/time.Second))*rate))
						expectedOkCnt := int64(math.Floor(0.5 + (float64(d/time.Second))*rate*(1.0-errRate)))
						tc :=
							testCase{
								name:                   fmt.Sprintf("rate%v_burst%v_runTime%v_errRate%v_jobTime%v_interval%v", rate, burst, d, errRate, jobTime, statInterval),
								burst:                  burst,
								rate:                   rate,
								RunPeriod:              d,
								RampDown:               rampDown,
								errRate:                errRate,
								StatInterval:           statInterval,
								JobTime:                jobTime,
								expectedStatCnt:        expectedStatCnt,
								ExpectedRunElapsedTime: time.Duration(expectedStatCnt) * statInterval,
								expectedOkCnt:          expectedOkCnt,
								expectedErrCnt:         expectedTotalReq - expectedOkCnt,
								expectedOkRate:         rate * (1.0 - errRate),
								expectedErrRate:        rate * errRate,
							}
						tt = append(tt, tc)
					}
				}
			}
		}
	}

	for _, tc := range tt {

		t.Run(tc.name, func(t *testing.T) {
			statChan := make(chan Stat, tc.expectedStatCnt)
			ctx := context.Background()
			l := log.New(os.Stderr, "", log.LstdFlags)
			go Run(ctx, l, tc.RunPeriod, tc.RampDown, tc.rate, tc.burst, tc.StatInterval, statChan, jobHandleFunc(tc.errRate, tc.JobTime))

			okCntTotal, errCntTotal := int64(0), int64(0)
			startTime := time.Now()

			stats := []Stat{}
			passed := true
			for s := range statChan {
				stats = append(stats, s)
				okCntTotal += s.OkCnt
				errCntTotal += s.ErrCnt
				okRate := s.OkCnt * int64(time.Second) / int64(s.IntervalSize)
				errRate := s.ErrCnt * int64(time.Second) / int64(s.IntervalSize)

				if len(stats) >= tc.expectedStatCnt { //last stat, ignore
					continue
				}

				if tc.JobTime > tc.RampDown {
					continue //some results will be discarded
				}
				if okRate != 0 && tc.expectedOkRate > 5 {
					epsilon := 0.5
					if tc.rate > 10 {
						epsilon /= math.Log10(tc.rate)
					}
					passed = passed && assert.InEpsilon(t, tc.expectedOkRate, okRate, epsilon,
						"okRate: %+v \n%+v \nexpected %v actual %v epsilon %v", tc, s, tc.expectedOkRate, okRate, epsilon)

				}
				if errRate != 0 && tc.expectedErrRate > 5 {
					epsilon := 1.0
					if tc.rate > 10 {
						epsilon /= math.Log10(tc.rate)
					}
					passed = passed && assert.InEpsilon(t, tc.expectedErrRate, errRate, epsilon,
						"errRate: %+v \n%+v \nexpected %v actual %v epsilon %v", tc, s, tc.expectedErrRate, errRate, epsilon)
					// log.Printf("%+v %+v errRate expected %v actual %v", tc, s, tc.expectedErrRate, errRate)
				}

				if s.OkCnt != 0 && tc.JobTime >= time.Millisecond {
					avgDoTime := s.SumDoTime / time.Duration(s.OkCnt)
					epsilon := 0.5
					if tc.rate > 10 {
						epsilon /= math.Log10(tc.rate)
					}
					//skill
					passed = passed && assert.InEpsilon(t, tc.JobTime, avgDoTime, epsilon, "avgDoTime: %+v \n%+v \nexpected %v actual %v epsilon %v", tc, s, tc.JobTime, avgDoTime, epsilon)
				}
			}

			elapsed := time.Since(startTime)

			statCntDelta := 1.0
			passed = passed && assert.InDelta(t, tc.expectedStatCnt, len(stats), statCntDelta,
				"stat cnt:%+v \nexpected%v,actual %v delta %v", tc, tc.expectedStatCnt, len(stats), statCntDelta)

			delta := tc.RampDown + tc.StatInterval
			if tc.JobTime > delta {
				delta = tc.JobTime + tc.StatInterval
			}

			passed = passed && assert.InDelta(t, int64(tc.ExpectedRunElapsedTime), int64(elapsed), float64(delta+500*time.Millisecond),
				"elapsed Runtime: %+v\n Expected %s,\t actual %s,\t delta %s", tc, tc.ExpectedRunElapsedTime, elapsed, delta+500*time.Millisecond)

			//when jobTime is greater than ramp downTime, job results discarded
			//we expected all job to be handled unless
			if okCntTotal != 0 && tc.expectedOkCnt > 10 && tc.JobTime < tc.RampDown {
				epsilon := 0.2
				if tc.rate > 10 {
					epsilon /= math.Log10(tc.rate)
				}
				passed = passed && assert.InEpsilon(t, tc.expectedOkCnt, okCntTotal, epsilon,
					"Total OK Cnt: %+v \n expected %v actual %v epsilon %v", tc, tc.expectedOkCnt, okCntTotal, epsilon)
			}
			if errCntTotal != 0 && tc.expectedErrCnt > 10 && tc.JobTime < tc.RampDown {
				epsilon := 2.0
				if tc.rate > 10 {
					epsilon /= math.Log10(tc.rate)
				}
				passed = passed && assert.InEpsilon(t, tc.expectedErrCnt, errCntTotal, epsilon,
					"Total errCnt:  %+v \n expected %v actual %v epsilon %v", tc, tc.expectedErrCnt, errCntTotal, epsilon)
			}

			if tc.JobTime < tc.RampDown {
				totalReq := okCntTotal + errCntTotal
				totalReqExpected := tc.expectedErrCnt + tc.expectedOkCnt
				passed = passed && assert.Equal(t, totalReqExpected, totalReq,
					"Total request:  %+v \n expected %v actual %v ", tc, totalReqExpected, totalReq)
			}

			if !passed {
				log.Printf("case %+v:\tElapsed time %v,\tTotal %v,\tdone %v,\terrors %v", tc.name, elapsed, okCntTotal+errCntTotal, okCntTotal, errCntTotal)
				printStats(stats)
			}

		})
	}

}

func printStats(stats []Stat) {
	for _, s := range stats {
		fmt.Printf("\n%+v\n", s)
	}
}
