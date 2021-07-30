package main

import (
	"bufio"
	"flag"
	"fmt"
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
	flag.StringVar(&logDir, "logDir", `/home/huyduong/Documents/logs`, "log_dir_path")
	// flag.StringVar(&logDir, "logDir", `/home/vunm/logs`, "log_dir_path")

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

	convert(fi, fo)
	fmt.Printf("file===%s", filepath)
}
func convert(inputFile *os.File, outputFile *os.File) {
	scanner := bufio.NewScanner(inputFile)
	for scanner.Scan() {
		line := scanner.Text()
		// time.Sleep(time.Second)
		fmt.Fprintln(outputFile, line)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
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
	fo, errCreateFile := os.Create(fmt.Sprintf("../../watch_file/test_watch/%s", fileInfo.Name()))
	if errCreateFile != nil {
		panic(errCreateFile)
	}
	return fo
}
