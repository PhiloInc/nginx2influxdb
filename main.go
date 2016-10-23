package main

import (
	"bufio"
	"flag"
	"log"
	"os"
	"time"
)

var (
	interval time.Duration
	batch  int

	tags map[string]string

	db *Database
)

func init() {
	var dbAddr, dbName, dbPassword, dbUser string
	var secsInt int
	var err error
	flag.IntVar(&secsInt, "i", 5, "Number of seconds between writes")
	flag.StringVar(&dbAddr, "a", "", "InfluxDB server address")
	flag.StringVar(&dbUser, "u", "", "InfluxDB database user")
	flag.StringVar(&dbPassword, "p", "", "InfluxDB database password")
	flag.StringVar(&dbName, "d", "", "InfluxDB database name")
	flag.IntVar(&batch, "b", 1000, "Batch size")
	flag.Parse()

	interval = time.Duration(secsInt) * time.Second

	db, err = NewDatabase(dbAddr, dbName, dbPassword, dbName)
	if err != nil {
		log.Printf("Could not connect to db: %v\n", err)
		os.Exit(1)
	}
}

func main() {
	var now time.Time
	scanner := bufio.NewScanner(os.Stdin)
	requests := make(Requests, 0, batch)
	last_push := time.Now()

	for scanner.Scan() {
		line := scanner.Text()

		req, err := NewRequest(line)
		if err != nil {
			log.Println(err)
			continue
		}

		requests = append(requests, req)

		now = time.Now()
		if now.Sub(last_push) >= interval || len(requests) >= batch {
			err = db.Write(requests)
			requests = make(Requests, 0, batch)
			last_push = now
		}
	}
}
