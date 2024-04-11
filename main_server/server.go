package main

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"main/utils"

	"main/firebaseutil"

	pb "github.com/ParkByeongKeun/trusafer-idl/maincontrol"
	"github.com/ParkByeongKeun/trusafer-idl/maincontrol/service"
	"github.com/dgrijalva/jwt-go"
	"github.com/google/uuid"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	"firebase.google.com/go/v4/messaging"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var saveImageDir string
var client mqtt.Client
var isPublished bool
var base_topic string
var logger utils.Logger
var mu sync.Mutex
var temp_min_array [9]float64
var temp_max_array [9]float64
var broker_mutex = &sync.Mutex{}
var (
	aesSecretKey []byte
)

var mEventEndTimes map[string]int64

func init() {
	mEventEndTimes = make(map[string]int64)
}

var mStatus map[string]string

func init() {
	mStatus = make(map[string]string)
}

var mImageStatus map[string]string

func init() {
	mImageStatus = make(map[string]string)
}

var mGroupUUIDs map[string][]string

func init() {
	mGroupUUIDs = make(map[string][]string)
}

type SensorData struct {
	Danger  []bool `json:"danger"`
	Warning []bool `json:"warning"`
}
type TempInit struct {
	Danger  []int `json:"danger"`
	Warning []int `json:"warning"`
}

type Claims struct {
	Email string `json:"email"`
	Admin bool   `json:"admin"`
	jwt.StandardClaims
}

var (
	firebase_client *messaging.Client
	firebase_ctx    context.Context = context.Background()
	tokenList       []string
)

func accessibleRolesForAT() map[string][]string {
	return map[string][]string{
		"admin": {"read", "write", "delete"},
		"user":  {"read"},
	}
}

func accessibleRolesForRT() map[string][]string {
	return map[string][]string{
		"admin": {"refresh"},
		"user":  {"refresh"},
	}
}

// firebase init
func initApp() {
	serviceAccountKeyPath := "/trusafer/serviceAccountKey.json"
	_, err := firebaseutil.InitApp(serviceAccountKeyPath)
	if err != nil {
		log.Fatalln("[initApp] initializing app error :", err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	initApp()

	conf_file := flag.String("config", "/trusafer/config.json", "config file path")
	flag.Parse()
	err := LoadConfiguration(*conf_file)
	if err != nil {
		log.Fatal(err)
	}

	// Connect to influxdb
	_influxdb_client = influxdb2.NewClient(Conf.InfluxDB.Address, Conf.InfluxDB.Token)
	_writeAPI = _influxdb_client.WriteAPIBlocking(Conf.InfluxDB.Org, Conf.InfluxDB.Bucket)
	_queryAPI = _influxdb_client.QueryAPI(Conf.InfluxDB.Org)
	defer _influxdb_client.Close()

	logFilePath := filepath.Join(Conf.Log.Dir, Conf.Log.File)
	lbj := &lumberjack.Logger{
		Filename:   logFilePath,
		MaxSize:    Conf.Log.MaxSize, // megabytes
		MaxBackups: Conf.Log.MaxBackups,
		MaxAge:     Conf.Log.MaxAge, //days
		LocalTime:  Conf.Log.LocalTime,
		Compress:   Conf.Log.Compress, // disabled by default
	}
	defer lbj.Close()
	logger = utils.NewLogger(lbj)
	logger.Title.Printf("Start Server")
	dbID := Conf.Database.Id
	dbPW := Conf.Database.Password
	dbAddr := Conf.Database.Address
	dbName := Conf.Database.Name
	grpcPort := Conf.Grpc.Port
	grpcCert := Conf.Grpc.CertFile
	grpcKey := Conf.Grpc.KeyFile
	saveImageDir = Conf.Etc.SaveImageDir
	gwPort := Conf.Gw.Port
	imPort := Conf.Im.Port
	brokerUserId := Conf.Broker.UserId
	brokerUserPassword := Conf.Broker.UserPassword
	secret_key_at := Conf.Jwt.SecretKeyAT
	token_duration_at := Conf.Jwt.TokenDurationAT

	hash := sha256.New()
	_, err = hash.Write([]byte(Conf.Encryption.Passphrase))
	if err != nil {
		log.Println(err.Error())
	}
	aesSecretKey = hash.Sum(nil)
	broker := "ssl://broker:1883"
	mqtt_serial := RandomString(15)
	base_topic = "trusafer"
	opts1 := mqtt.NewClientOptions().AddBroker(broker)
	tlsconfig := NewTLSConfig()
	opts1.SetClientID(mqtt_serial).SetTLSConfig(tlsconfig)
	opts1.SetUsername(brokerUserId)
	opts1.SetPassword(brokerUserPassword)
	opts1.SetCleanSession(false)

	opts1.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		println("mqtt connection lost error: " + err.Error())
	})
	opts1.SetReconnectingHandler(func(c mqtt.Client, options *mqtt.ClientOptions) {
		println("mqtt reconnecting...")
	})
	opts1.SetOnConnectHandler(func(c mqtt.Client) {
		println("mqtt connected!")

		if token := client.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}

		go func() {
			for msg := range messageCh {
				handleMessage(client, msg)
			}
		}()

		var wg sync.WaitGroup

		startSubscriber(client, base_topic+"/get/settop_sn/#", &wg)
		startSubscriber(client, base_topic+"/regist/sensor/#", &wg)
		startSubscriber(client, base_topic+"/deregist/sensor/#", &wg)
		startSubscriber(client, base_topic+"/data/frame/#", &wg)
		startSubscriber(client, base_topic+"/data/connection/#", &wg)
		startSubscriber(client, base_topic+"/data/status/#", &wg)
		startSubscriber(client, base_topic+"/data/info/#", &wg)

		wg.Wait()
	})

	client = mqtt.NewClient(opts1)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}
	dataSource := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbID, dbPW, dbAddr, dbName)
	db, err = sql.Open("mysql", dataSource)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatalf("DB ping errors: %s", err)
		return
	}
	log.Printf("Connected to DB [%s/%s] successfully\n", dbAddr, dbName)
	createDBTablesIfNotExist()
	certFile := grpcCert
	keyFile := grpcKey
	creds, sslErr := credentials.NewServerTLSFromFile(certFile, keyFile)
	if sslErr != nil {
		log.Fatalf("Failed loading certificates: %v", sslErr)
		return
	}
	opts := []grpc.ServerOption{}
	jwtManagerAT := service.NewJWTManager(secret_key_at, token_duration_at)
	interceptorAT := service.NewAuthInterceptor(jwtManagerAT, accessibleRolesForRT())
	opts = append(opts, grpc.Creds(creds))
	opts = append(opts, grpc.UnaryInterceptor(interceptorAT.Unary()))
	grpcServer := grpc.NewServer()
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	pb.RegisterMainControlServer(grpcServer, &server{})
	go func() {
		log.Printf("Serving gRPC server on %d port", grpcPort)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %s", err)
		}
	}()
	conn, err := grpc.DialContext(
		context.Background(),
		"0.0.0.0:"+strconv.Itoa(grpcPort),
		grpc.WithInsecure(),
	)

	if err != nil {
		log.Fatalln("Failed to dial server:", err)
	}
	gwmux := runtime.NewServeMux()

	err = pb.RegisterMainControlHandler(context.Background(), gwmux, conn)
	if err != nil {
		log.Fatalln("Failed to register gateway:", err)
	}
	gwPortString := fmt.Sprintf(":%d", gwPort)
	gwServer := &http.Server{
		Addr:    gwPortString,
		Handler: cors(gwmux),
	}

	go func() {
		log.Printf("Serving gRPC-Gateway on " + gwPortString + " port")
		log.Fatalln(gwServer.ListenAndServeTLS(certFile, keyFile))
	}()

	http.HandleFunc("/api/main/image/v1/upload_company_image", uploadHandler)
	http.HandleFunc("/api/main/image/v1/get_company_image", imageHandler)
	imPortString := fmt.Sprintf(":%d", imPort)
	httpServer := &http.Server{
		Addr:    imPortString,
		Handler: nil, // Use the default ServeMux
	}
	log.Printf("Serving Image REST API on " + imPortString + " port")
	log.Fatalln(httpServer.ListenAndServeTLS(certFile, keyFile))

}

func allowedOrigin(origin string) bool {
	if viper.GetString("cors") == "*" {
		return true
	}
	if matched, _ := regexp.MatchString(viper.GetString("cors"), origin); matched {
		return true
	}
	return false
}

func cors(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if allowedOrigin(r.Header.Get("Origin")) {
			w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, PUT")
			w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, Authorization, ResponseType")
		}
		if r.Method == "OPTIONS" {
			return
		}
		h.ServeHTTP(w, r)
	})
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		textValue := r.FormValue("company")
		userFolder := textValue
		err := os.MkdirAll(userFolder, os.ModePerm)
		if err != nil {
			log.Println("error creating user folder", err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		imageFileName := "company_logo" + ".jpg"
		imageFilePath := filepath.Join(userFolder, imageFileName)
		file, _, err := r.FormFile("file_image")
		if err != nil {
			log.Println("error retrieving file", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		dst, err := os.Create(imageFilePath)
		if err != nil {
			log.Println("error creating file", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer dst.Close()
		if _, err := io.Copy(dst, file); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "uploaded file: %s", imageFilePath)
	}
}

func imageHandler(w http.ResponseWriter, r *http.Request) {
	textValue := r.FormValue("company")
	filePath := textValue + "/company_logo.jpg"

	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "image/jpeg")
	if _, err := io.Copy(w, file); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}

func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}

func NewTLSConfig() *tls.Config {
	certpool := x509.NewCertPool()
	pemCerts, err := ioutil.ReadFile("cert/rootca.crt")
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	} else {
	}
	cert, err := tls.LoadX509KeyPair("cert/server.crt", "cert/private.pem")
	if err != nil {
		panic(err)
	}
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		panic(err)
	}
	return &tls.Config{
		RootCAs:            certpool,
		ClientAuth:         tls.NoClientCert,
		ClientCAs:          nil,
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{cert},
	}
}

func processMqttMessage(msg mqtt.Message, basePath string) {
	parts := strings.Split(msg.Topic(), "/")
	sensor_serial := parts[5]
	settop_serial := parts[3]
	mac := parts[4]
	formattedDate := time.Now().Format("2006-01-02")
	folderPath := filepath.Join(basePath, sensor_serial, formattedDate)
	err := createFolder(folderPath)
	if err != nil {
		log.Println("Error creating folder:", err)
		return
	}
	formattedTime := time.Now().Format("15:04:05")
	fileName := formattedTime + ".raw"
	filePath := filepath.Join(folderPath, fileName)
	err = saveImageToFile(filePath, msg.Payload(), settop_serial, mac, sensor_serial)
	if err != nil {
		log.Println("Error saving image:", err)
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

var pubMutex sync.Mutex

func publishStatus(data []byte, settop_serial, mac, sensor_serial string) error {
	mGroupUUIDs = make(map[string][]string)
	EVENT_DELAY := time.Duration(10)
	thresholds := getThresholdMapping(sensor_serial, sensor_serial)
	temp_min_array, temp_max_array, err := createImageFromData(data, 60, 50, 0)
	if err != nil {
		return errors.New("img field not found or not a string")
	}
	mImageStatus[sensor_serial] = "normal"
	for i, max_temp := range temp_max_array {
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
			} else {

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
			isCheck := duplicateCheckMessage(mImageStatus[sensor_serial], sensor_serial)
			if isCheck {
				return nil
			}
			set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
			pubMutex.Lock()
			client.Publish(set_topic, 1, false, frameJSON)
			pubMutex.Unlock()
			break
		}
	}
	var minValue, maxValue float64
	for i, value := range temp_max_array {
		if i == 0 {
			maxValue = value
		} else {
			maxValue = math.Max(maxValue, value)
		}
	}

	for i, value := range temp_min_array {
		if i == 0 {
			minValue = value
		} else {
			minValue = math.Min(minValue, value)
		}
	}

	fields := map[string]interface{}{
		"min_value": minValue,
		"max_value": maxValue,
	}
	getMutex.Lock()
	point := write.NewPoint(sensor_serial, nil, fields, time.Now())
	if err := _writeAPI.WritePoint(context.Background(), point); err != nil {
		log.Fatal(err)
	}
	getMutex.Unlock()
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

var saveMutex sync.Mutex

func saveImageToFile(filePath string, data []byte, settop_serial, mac, sensor_serial string) error {
	j_frame := map[string]interface{}{}
	err := json.Unmarshal(data, &j_frame)
	if err != nil {
		log.Println(err)
		return err
	}
	imgData, _ := base64.StdEncoding.DecodeString(j_frame["raw"].(string))
	saveMutex.Lock()
	publishStatus(imgData, settop_serial, mac, sensor_serial)
	saveMutex.Unlock()
	err = saveRawToFile(sensor_serial, imgData)
	if err != nil {
		log.Println("Error saving raw data to file:", err)
		return nil
	}
	return nil
}

func saveRawToFile(sensor_serial string, rawData []byte) error {
	encodedData := base64.StdEncoding.EncodeToString(rawData)
	fields := map[string]interface{}{
		"image_data": encodedData,
	}
	getMutex.Lock()
	point := write.NewPoint(sensor_serial, nil, fields, time.Now())
	if err := _writeAPI.WritePoint(context.Background(), point); err != nil {
		log.Fatal(err)
	}
	getMutex.Unlock()
	return nil
}

func createFolder(folderPath string) error {
	_, err := os.Stat(folderPath)
	if os.IsNotExist(err) {
		err := os.MkdirAll(folderPath, os.ModePerm)
		if err != nil {
			return err
		}
		log.Println("Folder created:", folderPath)
	} else if err != nil {
		return err
	}
	return nil
}

func SendMessageHandler(title string, body string, style string, topic string) {
	firebaseutil.SendMessageAsync(title, body, style, topic)
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

var messageCh = make(chan mqtt.Message, 1000)

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
		handleFrameData(client, parts, msg)

	case strings.HasPrefix(msg.Topic(), base_topic+"/data/connection/"):
		handleConnectionData(client, parts, payloadStr)

	case strings.HasPrefix(msg.Topic(), base_topic+"/data/status/"):
		handleStatusData(client, parts, payloadStr, msg)

	case strings.HasPrefix(msg.Topic(), base_topic+"/data/info/"):
		handleInfoData(client, parts, payloadStr, msg)
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
	fmt.Printf("%s 테이블이 성공적으로 생성되었습니다.\n", tableName)
	return nil
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

var frameMutex sync.Mutex

func handleFrameData(client mqtt.Client, parts []string, msg mqtt.Message) {
	basePath := "storage_data/"
	frameMutex.Lock()
	processMqttMessage(msg, basePath)
	frameMutex.Unlock()
	var decodedData map[string]interface{}
	err := json.Unmarshal(msg.Payload(), &decodedData)
	if err != nil {
		fmt.Println("JSON decoding error:", err)
		return
	}
}

var connectionMutex sync.RWMutex

func handleConnectionData(client mqtt.Client, parts []string, payloadStr string) {
	settop_serial := parts[3]
	mac := parts[4]
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
		WHERE mac = '%s'
	`, mac)
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

var statusMutex sync.Mutex

func handleStatusData(client mqtt.Client, parts []string, payloadStr string, msg mqtt.Message) {
	mGroupUUIDs = make(map[string][]string)
	settop_serial := parts[3]
	sensor_serial := parts[5]
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

			isCheck := duplicateCheckMessage(string(status), sensor_serial)
			if isCheck {
				return
			}
			query1 := fmt.Sprintf(`
		UPDATE sensor SET
			status = '%s'
		WHERE serial = '%s'
		`,
				strconv.Itoa(result), sensor_serial)
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
					statusMutex.Lock()
					SendMessageHandler("Trusafer", message, "style", group_uuid)
					statusMutex.Unlock()
				}
			}
			var group_master_uuid string
			query = fmt.Sprintf(`
			SELECT uuid 
			FROM group_ 
			WHERE name = 'master'
		`)
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
			statusMutex.Lock()
			SendMessageHandler("Trusafer", message, "style", group_master_uuid)
			statusMutex.Unlock()
			mainListMapping = NewMainListResponseMapping()
		}
	}
}

func handleInfoData(client mqtt.Client, parts []string, payloadStr string, msg mqtt.Message) {
	j_frame := map[string]interface{}{}
	err := json.Unmarshal(msg.Payload(), &j_frame)
	if err != nil {
		log.Println(err)
	}
	fw_version, _ := j_frame["fw_version"].(string)
	settop_serial := parts[3]
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

func createImageFromData(data []byte, width, height, startRow int) ([9]float64, [9]float64, error) {
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
	return min_arr, max_arr, nil
}
