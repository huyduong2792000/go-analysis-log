package main

// import (
// 	"flag"
// 	"io"
// 	"log"
// 	"os"
// 	"time"

// 	"github.com/satyrius/gonx"
// )

// var format string
// var logFile string

// func init() {
// 	flag.StringVar(&format, "format", `$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $rest_value`, "Nginx log_format name")
// 	flag.StringVar(&logFile, "logFile", `/home/huyduong/Documents/go-analysis-log/msoha-27-rq.log`, "log_file_path")
// }

// func main() {
// 	start := time.Now()
// 	file, err := os.Open(logFile)
// 	row_count := 0
// 	reader := gonx.NewReader(file, format)
// 	// var wg sync.WaitGroup
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()

// 	for {
// 		rec, err := reader.Read()
// 		if err == io.EOF {
// 			break
// 		}
// 		if rec != nil {
// 			// json.Marshal(rec.Fields())
// 			row_count++
// 		}
// 	}
// 	elapsed := time.Since(start)
// 	log.Printf("Binomial took %s with total row %d", elapsed, row_count)
// 	// wg.Wait()
// }
