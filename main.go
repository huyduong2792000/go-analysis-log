package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/satyrius/gonx"
	"github.com/segmentio/kafka-go"
)

const (
	topic         = "test-druid10"
	brokerAddress = "localhost:9092"
)

var format string
var logDir string

var writer *kafka.Writer

func init() {
	l := log.New(os.Stdout, "kafka writer: ", 0)
	flag.StringVar(&format, "format", `$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $rest_value`, "Nginx log_format name")
	// flag.StringVar(&logDir, "logDir", `/home/huyduong/Documents/logs`, "log_dir_path")
	// flag.StringVar(&logDir, "logDir", `/home/vunm/logs`, "log_dir_path")

	flag.StringVar(&logDir, "logDir", `/data/2021-03-21/data/2021-03-21`, "log_dir_path")

	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers:   []string{brokerAddress},
		Topic:     topic,
		BatchSize: 1,
		// BatchBytes:   1048576 * 10,
		// BatchTimeout: 2 * time.Second,
		RequiredAcks: -1,
		Logger:       l,
	})
}

func main() {
	ctx := context.Background()
	start := time.Now()
	parser := gonx.NewParser(format)
	for {
		files, err := ioutil.ReadDir(logDir)
		var wgHandleFile sync.WaitGroup
		if err != nil {
			panic(err)
		}
		// for {
		for _, file := range files {
			wgHandleFile.Add(1)
			filePath := logDir + "/" + file.Name()
			go HandleFile(filePath, parser, ctx, &wgHandleFile)
		}
		wgHandleFile.Wait()
		elapsed := time.Since(start)
		log.Printf("Binomial took %s", elapsed)
		// time.Sleep(time.Second * 60)
		// }
	}
}
func HandleFile(filepath string, parser *gonx.Parser, ctx context.Context, wgHandleFile *sync.WaitGroup) {
	defer wgHandleFile.Done()
	var wg sync.WaitGroup
	count := 0
	file, err := os.Open(filepath)
	reader := gonx.NewParserReader(file, parser)

	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if rec != nil {
			count++
			messageKey := fmt.Sprintf("%s-%d", filepath, count)
			// json.Marshal(rec.Fields())
			// go PubLishMessage(ctx, rec, &wg, messageKey)
			PubLishMessage(ctx, rec, messageKey)

		}
	}
	// fmt.Printf("file===%s, total_record=%d", filepath, count)
	wg.Wait()
}
func PubLishMessage(ctx context.Context, record *gonx.Entry, key string) {
	// wg.Add(1)
	// defer wg.Done()
	entryBytes, err := json.Marshal(record.Fields())
	if err != nil {
		log.Fatal(err)
	}

	errWrite := writer.WriteMessages(ctx, kafka.Message{
		// create an arbitrary message payload for the value
		Key:   []byte(key),
		Value: entryBytes,
	})
	if errWrite != nil {
		log.Fatal(errWrite)
	}
}
