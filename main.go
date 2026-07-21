package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

type Result struct {
	URL        string
	StatusCode int
	Duration   time.Duration
	Err        error
}

var (
	successCount int
	mu           sync.Mutex
)

func worker(id int, jobs <-chan string, results chan<- Result, wg *sync.WaitGroup) {
	defer wg.Done()

	for url := range jobs {
		result := checkURLSimple(url)
		if result.Err == nil && result.StatusCode == 200 {
			mu.Lock()
			successCount++
			mu.Unlock()
		}
		results <- result
	}
}

func checkURLSimple(url string) Result {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return Result{URL: url, Err: err, Duration: time.Since(start)}
	}

	resp, err := http.DefaultClient.Do(req)
	duration := time.Since(start)

	if err != nil {
		return Result{URL: url, Err: err, Duration: duration}
	}
	defer resp.Body.Close()

	return Result{URL: url, StatusCode: resp.StatusCode, Duration: duration}
}

func readURLs(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var urls []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" {
			urls = append(urls, line)
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return urls, nil
}

func main() {

	urls, err := readURLs("urls.txt")
	if err != nil {
		log.Fatal(err)
	}

	const numWorkers = 10

	jobs := make(chan string)
	results := make(chan Result)
	var wg sync.WaitGroup

	// 1. Запускаем фиксированное число воркеров
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		go worker(i, jobs, results, &wg)
	}

	// 2. Отправляем задачи в jobs (в отдельной горутине!)
	go func() {
		for _, url := range urls {
			jobs <- url
		}
		close(jobs)
	}()

	// 3. Горутина-наблюдатель — закрывает results, когда все воркеры закончили
	go func() {
		wg.Wait()
		close(results)
	}()

	start := time.Now()

	for result := range results {
		fmt.Printf("%s - status: %d, duration: %v, err: %v\n", result.URL, result.StatusCode, result.Duration, result.Err)
	}

	fmt.Printf("Total time: %v\n", time.Since(start))
	fmt.Printf("Successful checks: %d\n", successCount)
}
