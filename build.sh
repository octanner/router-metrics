#!/bin/sh

cd /go/src
go get "github.com/bsm/sarama-cluster"
go get "github.com/influxdata/influxdb/client/v2"
go get github.com/stackimpact/stackimpact-go
cd /go/src/router-metrics
go build router-metrics.go

