package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"sync"
	"time"

	"github.com/hpcloud/tail"
	"github.com/satyrius/gonx"
	"github.com/segmentio/kafka-go"
)

const (
	topic         = "test"
	brokerAddress = "localhost:9092"
)

var logDir string
var format string
var writer *kafka.Writer

func init() {
	flag.StringVar(&logDir, "logDir", `/home/huyduong/Documents/go-analysis-log/watch_file/test_watch`, "log_dir_path")
	flag.StringVar(&format, "format", `$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $rest_value`, "Nginx log_format name")
	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		BatchSize: 1,
		// BatchBytes:   1048576 * 10,
		BatchTimeout: time.Second / 2,
		RequiredAcks: -1,
		// Logger:       l,
	})
}
func main() {
	parser := gonx.NewParser(format)

	config := tail.Config{
		ReOpen:    false,                                // reopen
		Follow:    true,                                 // Whether to follow
		Location:  &tail.SeekInfo{Offset: 0, Whence: 2}, // Where to start reading in the file
		MustExist: false,                                // No error is reported if the file does not exist
		Poll:      true,
	}
	files, err := ioutil.ReadDir(logDir)
	ctx := context.Background()

	var wg sync.WaitGroup

	if err != nil {
		panic(err)
	}
	for _, file := range files {
		wg.Add(1)
		filePath := logDir + "/" + file.Name()
		go watch(filePath, config, &wg, parser, ctx, file.Name())
	}
	wg.Wait()
}

func watch(filePath string, config tail.Config, wg *sync.WaitGroup, parser *gonx.Parser, ctx context.Context, fileName string) {
	defer wg.Done()
	// count := 0
	// var line *tail.Line
	fileTail, _ := tail.TailFile(filePath, config)
	tailQueue := make(chan *tail.Tail)
	cancelChan := make(chan bool)

	go worker(tailQueue, cancelChan, parser, ctx)
	for {
		// line, _ = <-fileTail.Lines
		tailQueue <- fileTail
		// count++
		// if count >= 281665 {
		// 	fmt.Printf("\n===%d===%s===:", count, fileName)
		// }
	}
}

func worker(tailQueue chan *tail.Tail, cancelChan chan bool, parser *gonx.Parser, ctx context.Context) {
	// var line *tail.Line
	count := 0
	for {
		select {
		case <-cancelChan:
			return

		case fileTail := <-tailQueue:
			count++
			line, _ := <-fileTail.Lines
			entry, _ := parser.ParseString(line.Text)
			// _ = entry
			if entry != nil {
				messageKey := fmt.Sprintf("%s-%d", fileTail.Filename, count)
				entryBytes, err := json.Marshal(entry.Fields())
				if err != nil {
					log.Fatal(err)
				}
				errWrite := writer.WriteMessages(ctx, kafka.Message{
					// create an arbitrary message payload for the value
					Key:   []byte(messageKey),
					Value: entryBytes,
				})

				if errWrite != nil {
					log.Fatal(errWrite)
				}
			}

		}
	}
}
