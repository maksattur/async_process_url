package main

import (
	"bufio"
	"context"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

var workerCount = 300

func main() {
	start := time.Now()
	defer func() {
		log.Printf("total time: %v\n", time.Since(start))
	}()

	if len(os.Args) < 2 {
		log.Println("usage: go run main.go <file>")
		return
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	ch, err := readFile(ctx, os.Args[1])
	if err != nil {
		log.Printf("error reading file: %v\n", err)
		return
	}

	var wg sync.WaitGroup

	wg.Add(workerCount)

	for i := 1; i <= workerCount; i++ {
		go processWorker(ctx, i, &wg, ch)
	}

	wg.Wait()
}

func readFile(ctx context.Context, filePath string) (<-chan string, error) {
	out := make(chan string)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	scanner := bufio.NewScanner(file)

	go func() {
		defer func() {
			close(out)
			file.Close()
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
				if scanner.Scan() {
					out <- scanner.Text()
				} else {
					return
				}
			}
		}
	}()

	return out, nil
}

func fetch(ctx context.Context, workerID int, url string) {
	start := time.Now()

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		log.Printf("workerID : %d, failed to create request for url %s, error: %v\n", workerID, url, err)
		return
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("workerID : %d, failed to fetch url %s, error: %v\n", workerID, url, err)
		return
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("workerID : %d, failed to fetch url %s body, error: %v\n", workerID, url, err)
	}
	log.Printf("workerID : %d, url: %s, size: %d bytes, time of processing: %s\n", workerID, url, len(body), time.Since(start))
}

func processWorker(ctx context.Context, id int, wg *sync.WaitGroup, ch <-chan string) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case url, ok := <-ch:
			if !ok {
				return
			}
			fetch(ctx, id, url)
		}
	}
}
