// Command loadtest fires concurrent job-submission requests at a running
// task queue API and reports the real measured request rate and job
// completion throughput, by polling GET /api/v1/stats before and after.
//
// This exists so the README can report an actually-measured throughput
// number instead of the source PRD's unverified "10K+ jobs/sec" claim.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type stats struct {
	CompletedJobs int `json:"completed_jobs"`
}

func fetchStats(baseURL string) (stats, error) {
	resp, err := http.Get(baseURL + "/api/v1/stats")
	if err != nil {
		return stats{}, err
	}
	defer resp.Body.Close()
	var s stats
	if err := json.NewDecoder(resp.Body).Decode(&s); err != nil {
		return stats{}, err
	}
	return s, nil
}

func main() {
	baseURL := flag.String("url", "http://localhost:8030", "task queue API base URL")
	total := flag.Int("n", 2000, "total number of job submission requests to send")
	concurrency := flag.Int("c", 50, "number of concurrent submitters")
	waitCompletion := flag.Duration("wait", 20*time.Second, "how long to wait for jobs to drain before measuring completion throughput")
	flag.Parse()

	before, err := fetchStats(*baseURL)
	if err != nil {
		fmt.Printf("failed to fetch initial stats: %v\n", err)
		return
	}

	payload := []byte(`{"type":"data_transform","payload":{"numbers":[1,2,3],"operation":"sum"},"priority":5}`)

	var wg sync.WaitGroup
	jobsCh := make(chan struct{}, *total)
	for i := 0; i < *total; i++ {
		jobsCh <- struct{}{}
	}
	close(jobsCh)

	var successCount, failCount int
	var mu sync.Mutex

	start := time.Now()
	for i := 0; i < *concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 5 * time.Second}
			for range jobsCh {
				resp, err := client.Post(*baseURL+"/api/v1/jobs", "application/json", bytes.NewReader(payload))
				mu.Lock()
				if err != nil || resp.StatusCode != http.StatusCreated {
					failCount++
				} else {
					successCount++
				}
				mu.Unlock()
				if resp != nil {
					resp.Body.Close()
				}
			}
		}()
	}
	wg.Wait()
	submitDuration := time.Since(start)

	fmt.Printf("Submitted %d jobs (%d failed) in %s -> %.1f req/s\n",
		*total, failCount, submitDuration, float64(successCount)/submitDuration.Seconds())

	fmt.Printf("Waiting up to %s for jobs to complete...\n", *waitCompletion)
	time.Sleep(*waitCompletion)

	after, err := fetchStats(*baseURL)
	if err != nil {
		fmt.Printf("failed to fetch final stats: %v\n", err)
		return
	}

	completedDuring := after.CompletedJobs - before.CompletedJobs
	fmt.Printf("Completed %d jobs during the test window (%s) -> %.1f jobs/sec completion throughput\n",
		completedDuring, *waitCompletion, float64(completedDuring)/waitCompletion.Seconds())
}
