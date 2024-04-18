package main

import (
	"database/sql"
	"main/utils"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api"
)

// var db *sql.DB

var _influxdb_client influxdb2.Client
var _writeAPI api.WriteAPIBlocking
var _queryAPI api.QueryAPI

var client mqtt.Client

var messageCh = make(chan mqtt.Message, 1000)

var db *sql.DB
var base_topic string
var logger utils.Logger

var aesSecretKey []byte

var mEventEndTimes map[string]int64
var mStatus map[string]string
var mImageStatus map[string]string
var mGroupUUIDs map[string][]string
