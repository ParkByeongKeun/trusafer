package main

import (
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"main/firebaseutil"
	"math"
	"strconv"
	"strings"
	"sync"
	"time"

	pb "github.com/ParkByeongKeun/trusafer-idl/maincontrol"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
)

func startSubscriber(client mqtt.Client, topic string, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		if token := client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
			messageCh <- msg
		}); token.WaitTimeout(5*time.Second) && token.Error() != nil {
			fmt.Println(token.Error())
		}
	}()
}

func handleMessage(client mqtt.Client, msg mqtt.Message) {
	payloadStr := string(msg.Payload())
	parts := strings.Split(msg.Topic(), "/")

	switch {
	case strings.HasPrefix(msg.Topic(), base_topic+"/get/settop_sn/"):
		handleGetSettop_SN(client, parts, payloadStr)

	case strings.HasPrefix(msg.Topic(), base_topic+"/regist/sensor/"):
		handleRegistSensor(client, parts, payloadStr)

	case strings.HasPrefix(msg.Topic(), base_topic+"/deregist/sensor/"):
		handleDeregistSensor(client, parts)

	case strings.HasPrefix(msg.Topic(), base_topic+"/data/frame/"):
		handleFrameData(client, parts[3], parts[4], parts[5], msg)

	case strings.HasPrefix(msg.Topic(), base_topic+"/data/connection/"):
		handleConnectionData(client, parts[3], parts[4], payloadStr)

	case strings.HasPrefix(msg.Topic(), base_topic+"/data/status/"):
		handleStatusData(client, parts[3], parts[4], parts[5], payloadStr, msg)

	case strings.HasPrefix(msg.Topic(), base_topic+"/data/info/"):
		handleInfoData(client, parts[3], payloadStr, msg)
	}
}

func duplicateCheckMessage(status string, serial string) bool {
	var current_status string
	var result = -1
	var isCheck = false
	if string(status) == "normal" {
		result = 0
	} else if string(status) == "warning" {
		result = 1
	} else if string(status) == "danger" {
		result = 2
	} else if string(status) == "inspection" {
		result = 3
	}
	query := fmt.Sprintf(`
				SELECT status 
				FROM sensor 
				WHERE serial = '%s'
			`, serial)

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&current_status)
		if err != nil {
			log.Println(err)
		}
		if current_status == strconv.Itoa(result) {
			isCheck = true
		}
	}
	if isCheck {
		return true
	} else {
		return false
	}
}

var getMutex sync.RWMutex

func handleGetSettop_SN(client mqtt.Client, parts []string, payloadStr string) {

	var settop_serial string
	mac := parts[3]
	query := fmt.Sprintf(`
		SELECT serial 
		FROM settop 
		WHERE mac1 = '%s' OR mac2 = '%s'
	`,
		mac, mac)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&settop_serial)
		if err != nil {
			log.Println(err)
		}
	}
	if settop_serial == "" {
		log.Println("err settop_serial mac : ", mac)
		return
	}
	if mac == "" {
		log.Println("err mac empty")
		return
	}
	set_topic := base_topic + "/data/settop_sn/" + mac
	message := settop_serial
	log.Println(settop_serial)
	getMutex.Lock()
	client.Publish(set_topic, 1, false, message)
	getMutex.Unlock()
}

func serverLog(place string, floor string, room string, sensor_name string, sensor_serial string, type_ int) {
	formattedTime := time.Now().Format("2006-01-02 15:04:05")
	query := fmt.Sprintf(`
		INSERT INTO log_ SET
			place = '%s', 
			floor = '%s',
			room = '%s',
			sensor_name = '%s',
			sensor_serial = '%s',
			type = '%d',
			registered_time = '%s'
		`,
		place, floor, room, sensor_name, sensor_serial, type_, formattedTime)
	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer sqlAddRegisterer.Close()
}

func createSensorDataTable(db *sql.DB, tableName string) error {

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id int(11) NOT NULL AUTO_INCREMENT,
			min_temp FLOAT NOT NULL,
			max_temp FLOAT NOT NULL,
			date datetime NOT NULL,
			PRIMARY KEY (id),
			KEY fk_history_trusafer (id)
			) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`, tableName)
	_, err := db.Exec(query)
	if err != nil {
		return err
	}
	// fmt.Printf("%s 테이블이 성공적으로 생성되었습니다.\n", tableName)
	return nil
}


func initThreshold9Data(sensor_uuid string) {
	query := fmt.Sprintf(`
			INSERT IGNORE INTO threshold SET
				sensor_uuid = '%s', 
				temp_warning1 = '%s',
				temp_danger1 = '%s',
				temp_warning2 = '%s',
				temp_danger2 = '%s',
				temp_warning3 = '%s',
				temp_danger3 = '%s',
				temp_warning4 = '%s',
				temp_danger4 = '%s',
				temp_warning5 = '%s',
				temp_danger5 = '%s',
				temp_warning6 = '%s',
				temp_danger6 = '%s',
				temp_warning7 = '%s',
				temp_danger7 = '%s',
				temp_warning8 = '%s',
				temp_danger8 = '%s',
				temp_warning9 = '%s',
				temp_danger9 = '%s'
			`,
		sensor_uuid, strconv.Itoa(60), strconv.Itoa(100),
		strconv.Itoa(60), strconv.Itoa(100),
		strconv.Itoa(60), strconv.Itoa(100),
		strconv.Itoa(60), strconv.Itoa(100),
		strconv.Itoa(60), strconv.Itoa(100),
		strconv.Itoa(60), strconv.Itoa(100),
		strconv.Itoa(60), strconv.Itoa(100),
		strconv.Itoa(60), strconv.Itoa(100),
		strconv.Itoa(60), strconv.Itoa(100))
	sqlThresh, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer sqlThresh.Close()
}

var registMutex sync.Mutex

func handleRegistSensor(client mqtt.Client, parts []string, payloadStr string) {
	currentTime := time.Now()
	formattedTime := currentTime.Format("2006-01-02 15:04:05")
	var settop_uuid string
	var place_uuid string
	var place_name string
	var floor string
	var room string
	var uuid = uuid.New()
	settop_serial := parts[3]
	settopMac := parts[4]
	sensorSerial := parts[5]
	createSensorDataTable(db, sensorSerial)
	query := fmt.Sprintf(`
		SELECT uuid, place_uuid, floor, room  
		FROM settop 
		WHERE mac1 = '%s' OR mac2 = '%s'
	`,
		settopMac, settopMac)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&settop_uuid, &place_uuid, &floor, &room)
		if err != nil {
			log.Println(err)
		}
	}
	if settop_uuid == "" {
		log.Println("settop_uuid not found")
		return
	}
	query = fmt.Sprintf(`
			SELECT name 
			FROM place 
			WHERE uuid = '%s' 
		`,
		place_uuid)

	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&place_name)
		if err != nil {
			log.Println(err)
		}
	}

	query = fmt.Sprintf(`
		INSERT INTO sensor
			SET uuid = '%s', 
				settop_uuid = '%s',
				status = '%s',
				serial = '%s',
				ip_address = '%s',
				location = '%s',
				registered_time = '%s',
				mac = '%s',
				name = '%s',
				type = '%s'
				
		ON DUPLICATE KEY UPDATE
			settop_uuid = VALUES(settop_uuid),
			status = VALUES(status),
			ip_address = VALUES(ip_address),
			location = VALUES(location),
			registered_time = VALUES(registered_time),
			mac = VALUES(mac),
			type = VALUES(type)
	`,
		uuid.String(), settop_uuid, "0",
		sensorSerial, "", "",
		formattedTime, settopMac, sensorSerial, payloadStr,
	)
	sqlAddSensor, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	var get_sensor_uuid string
	var sensor_name string
	query = fmt.Sprintf(`
			SELECT uuid, name 
			FROM sensor 
			WHERE serial = '%s'
		`,
		sensorSerial)
	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&get_sensor_uuid, &sensor_name)
		if err != nil {
			log.Println(err)
		}
	}
	mainListMapping = NewMainListResponseMapping()
	initThreshold9Data(get_sensor_uuid)
	defer sqlAddSensor.Close()
	go serverLog(place_name, floor, room, sensor_name, sensorSerial, 1)
	set_topic := base_topic + "/get/info/" + settop_serial + "/" + settopMac
	message := "info"
	registMutex.Lock()
	client.Publish(set_topic, 1, false, message)
	registMutex.Unlock()

	if mStatus[sensorSerial] == "normal" {
		return
	}
	var group_uuid string
	query2 := fmt.Sprintf(`
		SELECT group_uuid
		FROM group_gateway
		WHERE settop_uuid = '%s'
	`, settop_uuid)

	rows2, err := db.Query(query2)
	if err != nil {
		log.Println(err)
	}
	defer rows2.Close()
	for rows2.Next() {
		err := rows2.Scan(&group_uuid)
		if err != nil {
			log.Println(err)
		}
		mGroupUUIDs[sensorSerial] = append(mGroupUUIDs[sensorSerial], group_uuid)
	}

	set_topic = base_topic + "/data/status/" + settop_serial + "/" + settopMac + "/" + sensorSerial
	j_frame := map[string]interface{}{
		"status":      "normal",
		"group_uuid":  mGroupUUIDs[sensorSerial],
		"sensor_name": sensor_name,
		"sensor_uuid": get_sensor_uuid,
		"settop_uuid": settop_uuid,
	}
	frameJSON, _ := json.Marshal(j_frame)
	connectionMutex.Lock()
	client.Publish(set_topic, 1, false, frameJSON)
	connectionMutex.Unlock()
	mStatus[sensorSerial] = "normal"
}


func handleDeregistSensor(client mqtt.Client, parts []string) {
	sensorSerial := parts[5]
	settopSerial := parts[3]
	settopMac := parts[4]
	log.Println("handleDeregistSensor called: ", sensorSerial)

	mainListMapping = NewMainListResponseMapping()
	var place_uuid string
	var place_name string
	var floor string
	var room string
	query := fmt.Sprintf(`
		SELECT place_uuid, floor, room  
		FROM settop 
		WHERE serial = '%s' 
	`,
		settopSerial)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&place_uuid, &floor, &room)
		if err != nil {
			log.Println(err)
		}
	}

	query = fmt.Sprintf(`
			SELECT name 
			FROM place 
			WHERE uuid = '%s' 
		`,
		place_uuid)
	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&place_name)
		if err != nil {
			log.Println(err)
		}
	}

	var sensor_name string
	var group_uuid string
	var sensor_uuid string
	var settop_uuid string

	query = fmt.Sprintf(`
				SELECT uuid, name, settop_uuid
				FROM sensor
				WHERE serial = '%s'
			`, sensorSerial)
	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&sensor_uuid, &sensor_name, &settop_uuid)
		if err != nil {
			log.Println(err)
		}

		query2 := fmt.Sprintf(`
						SELECT group_uuid
						FROM group_gateway
						WHERE settop_uuid = '%s'
					`, settop_uuid)

		rows2, err := db.Query(query2)
		if err != nil {
			log.Println(err)
		}
		defer rows2.Close()
		for rows2.Next() {
			err := rows2.Scan(&group_uuid)
			if err != nil {
				log.Println(err)
			}
			mGroupUUIDs[sensorSerial] = append(mGroupUUIDs[sensorSerial], group_uuid)
		}
		isCheck := duplicateCheckMessage("inspection", sensorSerial)
		if isCheck {
			continue
		}
		set_topic := base_topic + "/data/status/" + settopSerial + "/" + settopMac + "/" + sensorSerial
		j_frame := map[string]interface{}{
			"status":      "inspection",
			"group_uuid":  mGroupUUIDs[sensorSerial],
			"sensor_name": sensor_name,
			"sensor_uuid": sensor_uuid,
			"settop_uuid": settop_uuid,
		}
		frameJSON, _ := json.Marshal(j_frame)
		connectionMutex.Lock()
		client.Publish(set_topic, 1, false, frameJSON)
		go serverLog(place_name, floor, room, sensor_name, sensorSerial, 0)
		connectionMutex.Unlock()
	}
}

var frameMutex sync.Mutex

func saveFrame(sensor_serial string, rawData []byte, minValue, maxValue float64) error {
	encodedData := base64.StdEncoding.EncodeToString(rawData)
	fields := map[string]interface{}{
		"image_data": encodedData,
		"min_value":  minValue,
		"max_value":  maxValue,
	}

	point := write.NewPoint(sensor_serial, nil, fields, time.Now())
	influxdbQueueMtx.Lock()
	queue <- point
	influxdbQueueMtx.Unlock()
	return nil
}

func getThresholdMapping(key string, sensor_serial string) []*pb.Threshold {
	thresholdMappings, ok := thresholdMapping.GetThresholdMapping(key)
	if ok {
		return thresholdMappings
	} else {
		var sensor_uuid string
		query := fmt.Sprintf(`
			SELECT uuid
			FROM sensor 
			WHERE serial = '%s'
			`,
			sensor_serial)
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&sensor_uuid)
			if err != nil {
				log.Println(err)
			}
		}
		var thresholds []*pb.Threshold
		query = fmt.Sprintf(`
			SELECT temp_warning1, temp_danger1, temp_warning2, temp_danger2, temp_warning3,
				temp_danger3, temp_warning4, temp_danger4, temp_warning5, temp_danger5,
				temp_warning6, temp_danger6, temp_warning7, temp_danger7, temp_warning8,
				temp_danger8, temp_warning9, temp_danger9
			FROM threshold 
			WHERE sensor_uuid = '%s'
			`,
			sensor_uuid)

		rows, err = db.Query(query)
		if err != nil {
			log.Println(err)
			return nil
		}
		defer rows.Close()

		for rows.Next() {
			var tempWarning1, tempDanger1 string
			var tempWarning2, tempDanger2 string
			var tempWarning3, tempDanger3 string
			var tempWarning4, tempDanger4 string
			var tempWarning5, tempDanger5 string
			var tempWarning6, tempDanger6 string
			var tempWarning7, tempDanger7 string
			var tempWarning8, tempDanger8 string
			var tempWarning9, tempDanger9 string

			if err := rows.Scan(
				&tempWarning1, &tempDanger1,
				&tempWarning2, &tempDanger2,
				&tempWarning3, &tempDanger3,
				&tempWarning4, &tempDanger4,
				&tempWarning5, &tempDanger5,
				&tempWarning6, &tempDanger6,
				&tempWarning7, &tempDanger7,
				&tempWarning8, &tempDanger8,
				&tempWarning9, &tempDanger9,
			); err != nil {
				log.Println(err)
				return nil
			}

			threshold1 := &pb.Threshold{TempWarning: tempWarning1, TempDanger: tempDanger1}
			thresholds = append(thresholds, threshold1)
			threshold2 := &pb.Threshold{TempWarning: tempWarning2, TempDanger: tempDanger2}
			thresholds = append(thresholds, threshold2)
			threshold3 := &pb.Threshold{TempWarning: tempWarning3, TempDanger: tempDanger3}
			thresholds = append(thresholds, threshold3)
			threshold4 := &pb.Threshold{TempWarning: tempWarning4, TempDanger: tempDanger4}
			thresholds = append(thresholds, threshold4)
			threshold5 := &pb.Threshold{TempWarning: tempWarning5, TempDanger: tempDanger5}
			thresholds = append(thresholds, threshold5)
			threshold6 := &pb.Threshold{TempWarning: tempWarning6, TempDanger: tempDanger6}
			thresholds = append(thresholds, threshold6)
			threshold7 := &pb.Threshold{TempWarning: tempWarning7, TempDanger: tempDanger7}
			thresholds = append(thresholds, threshold7)
			threshold8 := &pb.Threshold{TempWarning: tempWarning8, TempDanger: tempDanger8}
			thresholds = append(thresholds, threshold8)
			threshold9 := &pb.Threshold{TempWarning: tempWarning9, TempDanger: tempDanger9}
			thresholds = append(thresholds, threshold9)

			thresholdMapping.AddThresholdMapping(key, thresholds)
		}
		return thresholds
	}
}

var pubMutex sync.Mutex

func setImageStatus(sensor_serial string, max_array [9]float64) {
	thresholds := getThresholdMapping(sensor_serial, sensor_serial)
	mImageStatus[sensor_serial] = "normal"
	for i, max_temp := range max_array {
		tempWarningStr := thresholds[i].GetTempWarning()
		tempDangerStr := thresholds[i].GetTempDanger()
		tempDangerFloat, _ := strconv.ParseFloat(tempDangerStr, 64)
		tempWarningFloat, _ := strconv.ParseFloat(tempWarningStr, 64)
		if float64(max_temp) > tempWarningFloat {
			mImageStatus[sensor_serial] = "warning"
		}
		if float64(max_temp) > tempDangerFloat {
			mImageStatus[sensor_serial] = "danger"
		}
	}
}

func publishStatus(min_array, max_array [9]float64, settop_serial, mac, sensor_serial string) error {
	mGroupUUIDs = make(map[string][]string)
	EVENT_DELAY := time.Duration(10)
	setImageStatus(sensor_serial, max_array)

	if mEventEndTimes[sensor_serial]+int64(EVENT_DELAY) > time.Now().Unix() { // 기존 이벤트 유지시간
		if mImageStatus[sensor_serial] != "normal" {
			if mStatus[sensor_serial] != mImageStatus[sensor_serial] {
				mStatus[sensor_serial] = mImageStatus[sensor_serial]
				mEventEndTimes[sensor_serial] = time.Now().Add(EVENT_DELAY).Unix()
				var settop_uuid string
				var group_uuid string
				var sensor_uuid string
				var sensor_name string

				query := fmt.Sprintf(`
					SELECT uuid 
					FROM settop 
					WHERE serial = '%s'
				`, settop_serial)

				rows1, err := db.Query(query)
				if err != nil {
					log.Println(err)
				}
				defer rows1.Close()
				for rows1.Next() {
					err := rows1.Scan(&settop_uuid)
					if err != nil {
						log.Println(err)
					}
					query2 := fmt.Sprintf(`
							SELECT group_uuid 
							FROM group_gateway 
							WHERE settop_uuid = '%s'
						`, settop_uuid)
					rows2, err := db.Query(query2)
					if err != nil {
						log.Println(err)
					}
					defer rows2.Close()
					for rows2.Next() {
						err := rows2.Scan(&group_uuid)
						if err != nil {
							log.Println(err)
						}
						mGroupUUIDs[sensor_serial] = append(mGroupUUIDs[sensor_serial], group_uuid)
					}
				}
				query = fmt.Sprintf(`
					SELECT uuid, name 
					FROM sensor 
					WHERE serial = '%s'
				`, sensor_serial)

				rows, err := db.Query(query)
				if err != nil {
					log.Println(err)
				}
				defer rows.Close()
				for rows.Next() {
					err := rows.Scan(&sensor_uuid, &sensor_name)
					if err != nil {
						log.Println(err)
					}
				}
				j_frame := map[string]interface{}{
					"status":      mImageStatus[sensor_serial],
					"group_uuid":  mGroupUUIDs[sensor_serial],
					"sensor_name": sensor_name,
					"sensor_uuid": sensor_uuid,
					"settop_uuid": settop_uuid,
				}
				frameJSON, _ := json.Marshal(j_frame)
				set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
				pubMutex.Lock()
				client.Publish(set_topic, 1, false, frameJSON)
				pubMutex.Unlock()
			} else { // alarm delay is added
				mEventEndTimes[sensor_serial] = time.Now().Add(EVENT_DELAY).Unix()
			}
		}
	} else { // publish new event or event expired(change to "normal")
		switch mImageStatus[sensor_serial] {
		case "normal":
			if mStatus[sensor_serial] != "normal" {
				mStatus[sensor_serial] = "normal"
				var settop_uuid string
				var group_uuid string
				var sensor_uuid string
				var sensor_name string

				query := fmt.Sprintf(`
					SELECT uuid 
					FROM settop 
					WHERE serial = '%s'
				`, settop_serial)

				rows1, err := db.Query(query)
				if err != nil {
					log.Println(err)
				}
				defer rows1.Close()
				for rows1.Next() {
					err := rows1.Scan(&settop_uuid)
					if err != nil {
						log.Println(err)
					}

					query2 := fmt.Sprintf(`
							SELECT group_uuid 
							FROM group_gateway 
							WHERE settop_uuid = '%s'
						`, settop_uuid)

					rows2, err := db.Query(query2)
					if err != nil {
						log.Println(err)
					}
					defer rows2.Close()
					for rows2.Next() {
						err := rows2.Scan(&group_uuid)
						if err != nil {
							log.Println(err)
						}
						mGroupUUIDs[sensor_serial] = append(mGroupUUIDs[sensor_serial], group_uuid)
					}
				}

				query = fmt.Sprintf(`
					SELECT uuid, name 
					FROM sensor 
					WHERE serial = '%s'
				`, sensor_serial)

				rows, err := db.Query(query)
				if err != nil {
					log.Println(err)
				}
				defer rows.Close()
				for rows.Next() {
					err := rows.Scan(&sensor_uuid, &sensor_name)
					if err != nil {
						log.Println(err)
					}
				}
				j_frame := map[string]interface{}{
					"status":      mImageStatus[sensor_serial],
					"group_uuid":  mGroupUUIDs[sensor_serial],
					"sensor_name": sensor_name,
					"sensor_uuid": sensor_uuid,
					"settop_uuid": settop_uuid,
				}
				frameJSON, _ := json.Marshal(j_frame)
				set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
				pubMutex.Lock()
				client.Publish(set_topic, 1, false, frameJSON)
				pubMutex.Unlock()
			}
			break
		case "warning":
			fallthrough
		case "danger":
			mStatus[sensor_serial] = mImageStatus[sensor_serial]
			mEventEndTimes[sensor_serial] = time.Now().Add(EVENT_DELAY).Unix()
			var settop_uuid string
			var sensor_uuid string
			var sensor_name string
			var group_uuid string
			query := fmt.Sprintf(`
				SELECT uuid 
				FROM settop 
				WHERE serial = '%s'
			`, settop_serial)

			rows1, err := db.Query(query)
			if err != nil {
				log.Println(err)
			}
			defer rows1.Close()
			for rows1.Next() {
				err := rows1.Scan(&settop_uuid)
				if err != nil {
					log.Println(err)
				}
				query2 := fmt.Sprintf(`
						SELECT group_uuid 
						FROM group_gateway 
						WHERE settop_uuid = '%s'
					`, settop_uuid)

				rows2, err := db.Query(query2)
				if err != nil {
					log.Println(err)
				}
				defer rows2.Close()

				for rows2.Next() {
					err := rows2.Scan(&group_uuid)
					if err != nil {
						log.Println(err)
					}
					mGroupUUIDs[sensor_serial] = append(mGroupUUIDs[sensor_serial], group_uuid)
				}
			}

			query = fmt.Sprintf(`
				SELECT uuid, name 
				FROM sensor 
				WHERE serial = '%s'
			`, sensor_serial)

			rows, err := db.Query(query)
			if err != nil {
				log.Println(err)
			}
			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(&sensor_uuid, &sensor_name)
				if err != nil {
					log.Println(err)
				}
			}
			j_frame := map[string]interface{}{
				"status":      mImageStatus[sensor_serial],
				"group_uuid":  mGroupUUIDs[sensor_serial],
				"sensor_name": sensor_name,
				"sensor_uuid": sensor_uuid,
				"settop_uuid": settop_uuid,
			}
			frameJSON, _ := json.Marshal(j_frame)

			set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
			pubMutex.Lock()
			client.Publish(set_topic, 1, false, frameJSON)
			pubMutex.Unlock()
			break
		}
	}
	return nil
}

func handleFrameData(client mqtt.Client, settop_serial string, mac string, sensor_serial string, msg mqtt.Message) {
	frameMutex.Lock()
	defer frameMutex.Unlock()

	j_frame := map[string]interface{}{}
	err := json.Unmarshal(msg.Payload(), &j_frame)
	if err != nil {
		log.Println(err)
		return
	}

	imgData, _ := base64.StdEncoding.DecodeString(j_frame["raw"].(string))
	min_array, max_array, minValue, maxValue, err := analizeFrame(imgData, 60, 50, 0)
	if err != nil {
		log.Println("img field not found or not a string")
		return
	}

	publishStatus(min_array, max_array, settop_serial, mac, sensor_serial)
	err = saveFrame(sensor_serial, imgData, minValue, maxValue)
	if err != nil {
		log.Println("Error saving raw data to file:", err)
		return
	}
}

var connectionMutex sync.RWMutex

func handleConnectionData(client mqtt.Client, settop_serial string, mac string, payloadStr string) {
	isalive := "0"
	if payloadStr == "0" {
		isalive = "0"
	} else {
		isalive = "1"
	}
	query := fmt.Sprintf(`
		UPDATE settop SET
			is_alive = '%s'
		WHERE serial = '%s'
		`,
		isalive, settop_serial)
	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
	}
	var status = ""
	if payloadStr == "0" {
		status = "inspection"

		var settop_uuid string
		var group_uuid string
		var sensor_uuid string
		var sensor_name string
		var sensor_serial string

		query = fmt.Sprintf(`
			SELECT uuid, name, serial, settop_uuid
			FROM sensor
			WHERE mac = '%s'`, mac)
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&sensor_uuid, &sensor_name, &sensor_serial, &settop_uuid)
			if err != nil {
				log.Println(err)
			}

			query2 := fmt.Sprintf(`
				SELECT group_uuid
				FROM group_gateway
				WHERE settop_uuid = '%s'
			`, settop_uuid)

			rows2, err := db.Query(query2)
			if err != nil {
				log.Println(err)
			}
			defer rows2.Close()
			for rows2.Next() {
				err := rows2.Scan(&group_uuid)
				if err != nil {
					log.Println(err)
				}
				mGroupUUIDs[sensor_serial] = append(mGroupUUIDs[sensor_serial], group_uuid)
			}
			isCheck := duplicateCheckMessage(status, sensor_serial)
			if isCheck {
				continue
			}
			set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
			j_frame := map[string]interface{}{
				"status":      status,
				"group_uuid":  mGroupUUIDs[sensor_serial],
				"sensor_name": sensor_name,
				"sensor_uuid": sensor_uuid,
				"settop_uuid": settop_uuid,
			}
			frameJSON, _ := json.Marshal(j_frame)
			connectionMutex.Lock()
			client.Publish(set_topic, 1, false, frameJSON)
			connectionMutex.Unlock()
			mainListMapping = NewMainListResponseMapping()
			mImageStatus[sensor_serial] = status
		}
	}
}

func eventMessage(settop_serial string, level_temp int, sensor_name string, sensor_serial string) string {
	var place_uuid string
	var room string
	var floor string
	var place_name string
	var message string
	msg_formattedTime := time.Now().Format("2006년 01월 02일 15시 04분 05초")
	query := fmt.Sprintf(`
		SELECT place_uuid, room, floor  
		FROM settop 
		WHERE serial = '%s' 
	`,
		settop_serial)

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&place_uuid, &room, &floor)
		if err != nil {
			log.Println(err)
		}
	}

	query = fmt.Sprintf(`
			SELECT name 
			FROM place 
			WHERE uuid = '%s' 
		`,
		place_uuid)

	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&place_name)
		if err != nil {
			log.Println(err)
		}
	}
	var message_lev string
	if level_temp == 1 {
		message_lev = "이상징후 (주의)"
	} else if level_temp == 2 {
		message_lev = "이상징후 (위험)"
	} else if level_temp == 3 {
		message_lev = "이상징후 (점검)"
	}

	go serverLog(place_name, floor, room, sensor_name, sensor_serial, level_temp+2)

	var checkEmptyPlace string
	var checkEmptyFloor string
	var checkEmptyRoom string
	if len(place_name) == 0 {
		checkEmptyPlace = ""
	} else {
		checkEmptyPlace = place_name + " "
		if len(floor) == 0 {
			checkEmptyFloor = ""
		} else {
			checkEmptyFloor = floor + "층 "
		}
		if len(room) == 0 {
			checkEmptyRoom = ""
		} else {
			checkEmptyRoom = room + "호실 "
		}
		checkEmptyPlace = checkEmptyPlace + checkEmptyFloor + checkEmptyRoom
	}
	message = msg_formattedTime + " " + checkEmptyPlace + sensor_name + " 센서에서 " + message_lev + "가 발견되었습니다. 해당 위치를 확인하시기 바랍니다."
	if level_temp == 0 {
		message = msg_formattedTime + " " + checkEmptyPlace + sensor_name + " 센서가 정상입니다."
	}
	return message
}

func handleStatusData(client mqtt.Client, settop_serial string, mac string, sensor_serial string, payloadStr string, msg mqtt.Message) {
	mGroupUUIDs = make(map[string][]string)

	j_frame := map[string]interface{}{}
	err := json.Unmarshal(msg.Payload(), &j_frame)
	var status string
	if err != nil { //sensor publish <<=
		status = string(msg.Payload())
	} else {
		status, _ = j_frame["status"].(string)

		var result = -1
		if string(status) == "normal" {
			result = 0
		} else if string(status) == "warning" {
			result = 1
		} else if string(status) == "danger" {
			result = 2
		} else if string(status) == "inspection" {
			isCheck := duplicateCheckMessage("inspection", sensor_serial)
			if isCheck {
				return
			}
			result = 3
		}
		mImageStatus[sensor_serial] = string(status)
		if result >= 0 {
			var sensor_name string
			query := fmt.Sprintf(`
				SELECT name 
				FROM sensor 
				WHERE serial = '%s'
			`, sensor_serial)

			rows, err := db.Query(query)
			if err != nil {
				log.Println(err)
			}
			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(&sensor_name)
				if err != nil {
					log.Println(err)
				}
			}
			query1 := fmt.Sprintf(`
								UPDATE sensor SET
									status = '%s'
								WHERE serial = '%s'
								`, strconv.Itoa(result), sensor_serial)
			_, err = db.Exec(query1)
			if err != nil {
				log.Println(err)
			}
			message := eventMessage(settop_serial, result, sensor_name, sensor_serial)
			var settop_uuid string
			var group_uuid string
			query = fmt.Sprintf(`
							SELECT uuid 
							FROM settop 
							WHERE serial = '%s'
							`, settop_serial)

			rows1, err := db.Query(query)
			if err != nil {
				log.Println(err)
			}
			defer rows1.Close()
			for rows1.Next() {
				err := rows1.Scan(&settop_uuid)
				if err != nil {
					log.Println(err)
				}

				query2 := fmt.Sprintf(`
									SELECT group_uuid 
									FROM group_gateway 
									WHERE settop_uuid = '%s'
								`, settop_uuid)

				rows2, err := db.Query(query2)
				if err != nil {
					log.Println(err)
				}
				defer rows2.Close()

				for rows2.Next() {
					err := rows2.Scan(&group_uuid)
					if err != nil {
						log.Println(err)
					}
					firebaseutil.SendMessageAsync("Trusafer", message, "style", group_uuid)
				}
			}
			var group_master_uuid string
			query = "SELECT uuid FROM group_ WHERE name = 'master'"
			rows, err = db.Query(query)
			if err != nil {
				log.Println(err)
			}
			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(&group_master_uuid)
				if err != nil {
					log.Println(err)
				}
			}
			firebaseutil.SendMessageAsync("Trusafer", message, "style", group_master_uuid)
			mainListMapping = NewMainListResponseMapping()
		}
	}
}

func handleInfoData(client mqtt.Client, settop_serial string, payloadStr string, msg mqtt.Message) {
	j_info := map[string]interface{}{}
	err := json.Unmarshal(msg.Payload(), &j_info)
	if err != nil {
		log.Println(err)
	}
	fw_version, _ := j_info["fw_version"].(string)
	query := fmt.Sprintf(`
		UPDATE settop SET
			fw_version = '%s'
		WHERE serial = '%s'
		`,
		fw_version, settop_serial)
	_, err = db.Exec(query)
	if err != nil {
		log.Println(err)
	}
	mainListMapping = NewMainListResponseMapping()
}

func analizeFrame(data []byte, width, height, startRow int) ([9]float64, [9]float64, float64, float64, error) {
	min := 255.
	max := 0.
	TEMP_MIN := 174.7
	// y 0~16, y 17~33, y 34~49,
	// x 0~19, 20~39, 40~59
	min_arr := [9]float64{999, 999, 999, 999, 999, 999, 999, 999, 999}
	max_arr := [9]float64{0, 0, 0, 0, 0, 0, 0, 0, 0}
	for y := 0; y < height-startRow; y++ {
		for x := 0; x < width; x++ {
			index := ((startRow+y)*width + x) * 2
			pixelData := data[index : index+2]
			rawValue := binary.BigEndian.Uint16(pixelData)
			temp := ((float64(rawValue)-2047)/10. + 30) + TEMP_MIN
			if temp < min {
				min = temp
			}
			if temp > max {
				max = temp
			}
			var row int
			if y < int(math.Round(float64(height/3.))) {
				row = 0
			} else if y < int(math.Round(float64(height*2/3.))) {
				row = 1
			} else {
				row = 2
			}

			idx := 0
			if x < int(math.Round(float64(width/3.))) {
				idx = row * 3
			} else if x < int(math.Round(float64(width*2/3.))) {
				idx = row*3 + 1
			} else {
				idx = row*3 + 2
			}

			if max_arr[idx] < temp {
				max_arr[idx] = temp
			}
			if min_arr[idx] > temp {
				min_arr[idx] = temp
			}
		}
	}
	for i, value := range max_arr {
		max_arr[i] = math.Round((value-TEMP_MIN)*10) / 10
	}
	for i, value := range min_arr {
		min_arr[i] = math.Round((value-TEMP_MIN)*10) / 10
	}

	var minValue, maxValue float64
	for i, value := range max_arr {
		if i == 0 {
			maxValue = value
		} else {
			maxValue = math.Max(maxValue, value)
		}
	}

	for i, value := range min_arr {
		if i == 0 {
			minValue = value
		} else {
			minValue = math.Min(minValue, value)
		}
	}

	return min_arr, max_arr, minValue, maxValue, nil
}

