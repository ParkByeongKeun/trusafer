package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

var queue chan *write.Point

const (
	queueSize   = 1000
	flushPeriod = 1 * time.Second
)

var influxdbQueueMtx sync.Mutex

func writeQueue() {
	ticker := time.NewTicker(flushPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			flushQueue()
		}
	}
}

func flushQueue() {
	influxdbQueueMtx.Lock()
	var pointsToWrite []*write.Point
	for len(queue) > 0 {
		point := <-queue
		pointsToWrite = append(pointsToWrite, point)
	}
	influxdbQueueMtx.Unlock()

	if len(pointsToWrite) > 0 {
		logger.Info.Printf("queue len : %d", len(pointsToWrite))
		if Conf.InfluxDB.IsLog {
			log.Println("queue len : ", len(pointsToWrite))
		}
		if err := _writeAPI.WritePoint(context.Background(), pointsToWrite...); err != nil {
			logger.Error.Printf("Failed to write to InfluxDB: %s", err)
			log.Println(err)
		}
	}
}
