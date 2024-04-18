package main

import (
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// var db *sql.DB

var _influxdb_client influxdb2.Client
var _writeAPI api.WriteAPIBlocking
var _queryAPI api.QueryAPI

// var client mqtt.Client
// var isPublished bool
// var base_topic string
// var logger utils.Logger
// var mu sync.Mutex
// var broker_mutex = &sync.Mutex{}
// var messageCh = make(chan mqtt.Message, 1000)
