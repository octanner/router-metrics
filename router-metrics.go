package main

import (
	"fmt"
	cluster "github.com/bsm/sarama-cluster"
	client "github.com/influxdata/influxdb/client/v2"
	"github.com/stackimpact/stackimpact-go"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"
)

var conn net.Conn
var err error
var processed int
var errors int
var debugmode bool
var database_name string
var c client.Client

func main() {
	agent := stackimpact.Start(stackimpact.Options{
		AgentKey: os.Getenv("STACKIMPACT_KEY"),
		AppName:  "router-metrics",
	})
	fmt.Println(agent)

	processed = 0
	errors = 0
	envdebug := os.Getenv("DEBUG")
	if envdebug == "" {
		envdebug = "false"
	}
	debugmode, err = strconv.ParseBool(envdebug)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Printf("debug mode: %v\n", debugmode)
	database_name = os.Getenv("DATABASE_NAME")
	c, err = client.NewHTTPClient(client.HTTPConfig{
		Addr: os.Getenv("INFLUX_URL"),
	})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	config := cluster.NewConfig()
	config.Consumer.Return.Errors = true
	config.Group.Return.Notifications = true
	brokers := strings.Split(os.Getenv("KAFKA_BROKERS"), ",")
	topics := []string{"alamoweblogs"}
	consumer, err := cluster.NewConsumer(brokers, os.Getenv("CONSUMER_GROUP_NAME"), topics, config)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	defer consumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	signal.Notify(signals, os.Kill)

	consumed := 0

	for {
		select {
		case msg, more := <-consumer.Messages():
			if more {
				sock(string(msg.Value[:]))
				consumer.MarkOffset(msg, "")
				consumed++
			}
		case err, more := <-consumer.Errors():
			if more {
				log.Printf("Error: %s\n", err.Error())
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				log.Printf("Rebalanced: %+v\n", ntf)
			}
		case <-signals:
			return
		}
	}

	log.Printf("Consumed: %d\n", consumed)
	log.Printf("Processed: %d\n", processed)
	conn.Close()

}

func addPoint(bp client.BatchPoints, metricname string, value string, tags map[string]string) (bpback client.BatchPoints) {
	valuei, err := strconv.ParseFloat(value, 64)
	if err != nil {
		fmt.Println(err)
		return bp
	}
	fields := map[string]interface{}{
		"value": valuei,
	}
	pt, err := client.NewPoint(
		metricname,
		tags,
		fields,
		time.Now(),
	)
	if err != nil {
		fmt.Println(err)
		return bp
	}
	bp.AddPoint(pt)
	return bp

}

func sock(logline string) {
	words := strings.Fields(logline)
	var fieldmap map[string]string
	fieldmap = make(map[string]string)
	if debugmode {
		fmt.Printf("%+v\n", logline)
		fmt.Printf("%+v\n", words)
	}

	for _, element := range words {
		if strings.Contains(element, "=") {
			fieldmap[strings.Split(element, "=")[0]] = strings.Split(element, "=")[1]
		}
	}
	if debugmode {
		fmt.Printf("%+v\n", fieldmap)
	}
	host := fieldmap["hostname"]
	if !(strings.HasPrefix(host, "alamotest")) && len(words) > 9 && !strings.Contains(strings.Join(words, " "), "4813") {
		bp, err := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  database_name,
			Precision: "us",
		})
		if err != nil {
			fmt.Println(err)
		}
		site_domain := fieldmap["site_domain"]
		var tags map[string]string
		tags = make(map[string]string)
		tags["host"] = host
		bp = addPoint(bp, "router.service.ms", fieldmap["service"], tags)
		bp = addPoint(bp, "router.connect.ms", fieldmap["connect"], tags)
		bp = addPoint(bp, "router.total.ms", fieldmap["total"], tags)
		bp = addPoint(bp, "router.status."+fieldmap["status"], "1", tags)
		bp = addPoint(bp, "router.requests.count", "1", tags)
		processed++
		if site_domain != "" {
			tags["host"] = site_domain
			bp = addPoint(bp, "router.service.ms", fieldmap["service"], tags)
			bp = addPoint(bp, "router.connect.ms", fieldmap["connect"], tags)
			bp = addPoint(bp, "router.total.ms", fieldmap["total"], tags)
			bp = addPoint(bp, "router.status."+fieldmap["status"], "1.0", tags)
			bp = addPoint(bp, "router.requests.count", "1.0", tags)
			processed++
		}
		if !debugmode {
			go sendtoinflux(bp)
		}
	}
	//fmt.Printf("processed: %v errors: %v\n", processed,errors)
	//if processed > 100000{
	//        os.Exit(0)
	//}
}

func sendtoinflux(bp client.BatchPoints) {
	//	fmt.Printf("shipping %+v\n", bp)
	if err := c.Write(bp); err != nil {
		fmt.Println(err)
		errors++
	}

}
