package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"

	"github.com/satyrius/gonx"
)

// const (
// topic         = "test-druid7"
// brokerAddress = "localhost:9092"
// )

var format string
var logDir string

func init() {
	// l := log.New(os.Stdout, "kafka writer: ", 0)
	flag.StringVar(&format, "format", `$remote_addr $http_x_forwarded_for [$time_iso8601] $http_host "$request" $status $bytes_sent "$http_referer" "$http_user_agent" $rest_value`, "Nginx log_format name")
	// flag.StringVar(&logDir, "logDir", `/home/huyduong/Documents/logs`, "log_dir_path")
	flag.StringVar(&logDir, "logDir", `/home/vunm/logs`, "log_dir_path")

}

func main() {
	start := time.Now()
	// ctx := context.Background()
	parser := gonx.NewParser(format)
	files, err := ioutil.ReadDir(logDir)
	var wg sync.WaitGroup
	if err != nil {
		panic(err)
	}
	for _, file := range files {
		wg.Add(1)
		go HandleFile(file, parser, &wg)
	}
	wg.Wait()
	elapsed := time.Since(start)
	log.Printf("Binomial took %s", elapsed)
}
func HandleFile(fileInfo fs.FileInfo, parser *gonx.Parser, wg *sync.WaitGroup) {
	defer wg.Done()
	filepath := logDir + "/" + fileInfo.Name()
	fi := openInputFile(fileInfo)
	defer func() {
		if err := fi.Close(); err != nil {
			panic(err)
		}
	}()

	fo := openOutputFIle(fileInfo)
	defer func() {
		if err := fo.Close(); err != nil {
			panic(err)
		}
	}()
	reader := gonx.NewParserReader(fi, parser)

	convert(fi, fo, reader)
	fmt.Printf("file===%s", filepath)
}
func convert(inputFile *os.File, outputFile *os.File, reader *gonx.Reader) {
	var wg sync.WaitGroup

	for {
		rec, err := reader.Read()
		if err == io.EOF {
			break
		}
		if rec != nil {
			wg.Add(1)
			go func() {
				recConvered, _ := json.Marshal(rec.Fields())
				breakLine := []byte("\n")
				recConvered = append(recConvered, breakLine...)
				if _, err := outputFile.Write(recConvered); err != nil {
					panic(err)
				}
				wg.Done()
			}()

		}
	}
	wg.Wait()
}
func openInputFile(fileInfo fs.FileInfo) *os.File {
	filepath := logDir + "/" + fileInfo.Name()
	file, errOpenFile := os.Open(filepath)
	if errOpenFile != nil {
		panic(errOpenFile)
	}
	return file
}

func openOutputFIle(fileInfo fs.FileInfo) *os.File {
	fo, errCreateFile := os.Create(fmt.Sprintf("../%s/%s.json", "output_convert", fileInfo.Name()))
	if errCreateFile != nil {
		panic(errCreateFile)
	}
	return fo
}
