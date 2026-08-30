package main
// # go get github.com/valyala/fasthttp
// # go run server.go
// # wrk -t8 -c200 -d30s http://localhost:3000


import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/valyala/fasthttp"
)

// requestCount is a global, atomic counter for requests.
var requestCount uint64

// helloWorldPayload is the pre-rendered response.
var helloWorldPayload = []byte("Hello, World!")

// metricsMiddleware is a middleware that counts every request.
func metricsMiddleware(next fasthttp.RequestHandler) fasthttp.RequestHandler {
	return func(ctx *fasthttp.RequestCtx) {
		// Atomically increment the counter for each incoming request.
		atomic.AddUint64(&requestCount, 1)
		// Call the next handler in the chain.
		next(ctx)
	}
}

func helloHandler(ctx *fasthttp.RequestCtx) {
	// fasthttp is efficient, but we can still help by setting the content type
	// and writing the pre-allocated byte slice.
	ctx.SetContentType("text/plain; charset=utf-8")
	ctx.Write(helloWorldPayload)
}

// reportThroughput periodically prints the throughput (requests per second).
func reportThroughput(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for range ticker.C {
		count := atomic.SwapUint64(&requestCount, 0)
		rps := float64(count) / interval.Seconds()
		log.Printf("Current throughput: %.2f req/s\n", rps)
	}
}

func main() {
	// Define a command-line flag for the port.
	port := flag.String("port", "3000", "Port for the server to listen on")
	flag.Parse()

	listenAddr := ":" + *port

	// Start a separate goroutine that measures and prints the throughput.
	go reportThroughput(5 * time.Second)

	// Wrap the final handler with the middleware.
	finalHandler := metricsMiddleware(helloHandler)

	fmt.Printf("fasthttp server starting on http://0.0.0.0:%s\n", *port)

	if err := fasthttp.ListenAndServe(listenAddr, finalHandler); err != nil {
		log.Fatalf("Error in ListenAndServe: %s", err)
	}
}
