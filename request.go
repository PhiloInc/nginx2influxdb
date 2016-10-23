package main

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"
)

// Regular expression to match standard nginx access log outout with request_time
// inserted after status
var logRegexp = regexp.MustCompile(`(\S+) - (\S+) \[([^\]]+)\] "([^"]+)" (\S+) (\S+) (\S+) "([^"]*?)" "([^"]*?)"( "([^"]*?)")?`)

type Request struct {
	Ip        string    // Remote IP address of the client
	Proto     string    // HTTP protocol
	Method    string    // Request method (GET, POST, etc)
	Host      string    // Requested hostname
	Path      string    // Requested path
	Status    string    // Responses status code (200, 400, etc)
	Referer   string    // Referer (usually is set to "-")
	Agent     string    // User agent string
	BytesSent string    // How many bytes sent?
	ReqTime   string    // How long did it take to service the request?
	Timestamp time.Time // Request timestamp (UTC)
}

type Requests []*Request

// Parse nginx request data
// Example: "GET http://foobar.com/ HTTP/1.1"
func parseRequest(str string, req *Request) error {
	chunks := strings.Split(str, " ")
	if len(chunks) != 3 {
		return fmt.Errorf("invalid request format")
	}

	req.Method = chunks[0]
	req.Proto = chunks[2]

	if uri, err := url.Parse(chunks[1]); err == nil {
		req.Host = uri.Host
		req.Path = uri.Path
	}

	return nil
}

// Parse nginx log timestamp
// Example: 21/Mar/2016:02:33:29 +0000
func parseTimestamp(str string, req *Request) error {
	ts, err := time.Parse("02/Jan/2006:15:04:05 -0700", str)
	if err == nil {
		req.Timestamp = ts
	}
	return err
}

// Produce fields for consumption by influxdb
func (r *Request) InfluxFields() map[string]interface{} {
	return map[string]interface{}{
		"proto": r.Proto,
		"method": r.Method,
		"path": r.Path,
		"status": r.Status,
		"agent": r.Agent,
		"bytes_sent": r.BytesSent,
		"req_time": r.ReqTime,
	}
}

// Initialize a new request from the input string
func NewRequest(str string) (*Request, error) {
	allmatches := logRegexp.FindAllStringSubmatch(str, -1)
	if len(allmatches) == 0 {
		return &Request{}, fmt.Errorf("no matches")
	}
	matches := allmatches[0]

	req := &Request{
		Ip:      matches[1],
		Status:  matches[5],
		BytesSent: matches[6],
		ReqTime: matches[7],
		Referer: matches[8],
		Agent:   matches[9],
	}

	parseTimestamp(matches[3], req)
	parseRequest(matches[4], req)
	fmt.Printf("%+v\n", req)
	fmt.Println(str)
	return req, nil
}
