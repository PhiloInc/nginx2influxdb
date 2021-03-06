package main

import (
	"fmt"
	"log"
	"github.com/influxdata/influxdb/client/v2"
	"time"
)

const (
	SERIES_NAME = "nginx_requests"
)

func createDB(clnt client.Client, name string) {
	q := client.Query{
		Command: fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name),
	}

	if response, err := clnt.Query(q); err == nil {
		if response.Error() != nil {
			log.Printf("Db creation error: %v", response.Error())
		}
	} else {
		log.Println(err)
	}
}

type Database struct {
	Client client.Client
	Name string
}

func NewDatabase(addr, username, password, name string) (*Database, error) {
	client, err := client.NewHTTPClient(client.HTTPConfig{
		Addr: addr,
		Username: username,
		Password: password,
		Timeout: time.Duration(100 * time.Millisecond),
	})
	if err != nil {
		return &Database{}, err
	}
	createDB(client, name)
	return &Database{client, name}, nil
}

func (db Database) Write(requests Requests) error {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  db.Name,
		Precision: "us",
	})

	for _, r := range requests {
		pt, _ := client.NewPoint(
			SERIES_NAME,
			r.InfluxTags(),
			r.InfluxFields(),
			r.Timestamp,
		)
		bp.AddPoint(pt)
	}

	return db.Client.Write(bp)
}
