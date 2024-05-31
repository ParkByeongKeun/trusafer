package main

import (
	"container/list"
	"context"
	"log"
	"sync"
	"time"

	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

var queue Queue

func NewQueue() *Queue {
	return &Queue{list.New()}
}

type Queue struct {
	v *list.List
}

func (q *Queue) Push(v interface{}) {
	q.v.PushBack(v)
}

func (q *Queue) Pop() interface{} {
	front := q.v.Front()
	if front == nil {
		return nil
	}

	return q.v.Remove(front)
}

func (q *Queue) Size() int {
	return q.v.Len()
}

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
	for queue.Size() > 0 {
		_point := queue.Pop()
		point := _point.(*write.Point)
		pointsToWrite = append(pointsToWrite, point)
		if Conf.InfluxDB.IsLog {
			log.Println("queue len : ", len(pointsToWrite))
		}
	}
	influxdbQueueMtx.Unlock()

	for _, point := range pointsToWrite {
		if err := _writeAPI.WritePoint(context.Background(), point); err != nil {
			logger.Error.Printf("Failed to write to InfluxDB: %s", err)
			log.Println(err)
		}
	}
}
