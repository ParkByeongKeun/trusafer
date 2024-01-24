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
	"sort"
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
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"

	_ "github.com/go-sql-driver/mysql"
	// "github.com/h2non/bimg"
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

// sensor history data
type HistoryData struct {
	SensorSerial  string
	MinValue      float64
	MaxValue      float64
	FormattedTime string
}

type SensorQueue struct {
	queue []HistoryData
	mu    sync.Mutex
}

func (q *SensorQueue) Enqueue(data HistoryData) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue = append(q.queue, data)

}

func (q *SensorQueue) DequeueAll() []HistoryData {
	q.mu.Lock()
	defer q.mu.Unlock()

	dataCopy := make([]HistoryData, len(q.queue))
	copy(dataCopy, q.queue)
	q.queue = nil
	return dataCopy
}

var queue = SensorQueue{}

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
	// serviceAccountKeyPath := "./serviceAccountKey.json"
	serviceAccountKeyPath := "/trusafer/serviceAccountKey.json"
	_, err := firebaseutil.InitApp(serviceAccountKeyPath)
	if err != nil {
		log.Fatalln("[initApp] initializing app error :", err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())
	initApp()
	// conf_file := flag.String("config", "/Users/bkpark/works/go/trusafer/main_server/config.json", "config file path")
	conf_file := flag.String("config", "/trusafer/config.json", "config file path")
	flag.Parse()
	err := LoadConfiguration(*conf_file)
	if err != nil {
		log.Fatal(err)
	}
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
	// secret_key_rt := Conf.Jwt.SecretKeyRTrf crtuy
	secret_key_at := Conf.Jwt.SecretKeyAT
	token_duration_at := Conf.Jwt.TokenDurationAT
	// token_duration_rt := Conf.Jwt.TokenDurationRT

	hash := sha256.New()
	_, err = hash.Write([]byte(Conf.Encryption.Passphrase))
	if err != nil {
		log.Println(err.Error())
	}
	aesSecretKey = hash.Sum(nil)

	// broker := "ssl://192.168.13.5:21984"
	broker := "ssl://broker:1883"
	mqtt_serial := RandomString(15)
	username := "ijoon"
	password := "vXH5iVMqTfXB"
	// password := "9DGQhyCH6RZ4"
	base_topic = "trusafer"
	opts1 := mqtt.NewClientOptions().AddBroker(broker)

	tlsconfig := NewTLSConfig()

	opts1.SetClientID(mqtt_serial).SetTLSConfig(tlsconfig)
	opts1.SetUsername(username)
	opts1.SetPassword(password)
	opts1.SetCleanSession(true)
	opts1.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		println("mqtt connection lost error: " + err.Error())
	})
	opts1.SetReconnectingHandler(func(c mqtt.Client, options *mqtt.ClientOptions) {
		println("mqtt reconnecting...")
	})

	opts1.SetOnConnectHandler(func(c mqtt.Client) {
		println("mqtt connected!")
		var wg sync.WaitGroup
		startSubscriber := func(client mqtt.Client, topic string) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				subscribeHandler(client, topic)
			}()
		}

		startSubscriber(client, base_topic+"/get/settop_sn/#")
		startSubscriber(client, base_topic+"/regist/sensor/#")
		startSubscriber(client, base_topic+"/deregist/sensor/#")
		// wg.Add(1)
		// go subscribeHandler(client, base_topic+"/data/threshold9/#")
		startSubscriber(client, base_topic+"/data/frame/#")
		startSubscriber(client, base_topic+"/data/connection/#")
		startSubscriber(client, base_topic+"/data/status/#")
		startSubscriber(client, base_topic+"/data/info/#")

		wg.Wait()
	})
	//file_ delete
	basePath := "storage_data/"
	go deleteOldImages(basePath)

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

	// creds2, sslErr := credentials.NewClientTLSFromFile(certFile, "127.0.0.1")
	// if sslErr != nil {
	// 	log.Fatalf("Failed loading certificates: %v", sslErr)
	// 	return
	// }
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
		for {
			time.Sleep(30 * time.Second)

			// broker_mutex.Lock()
			set_topic := base_topic + "/gget/connection"
			client.Publish(set_topic, 1, false, "")

			// go func() {
			// 	_ = pub_token.Wait()
			// 	if pub_token.Error() != nil {
			// 		log.Println(pub_token.Error())
			// 	}
			// }()
			// broker_mutex.Unlock()
		}
	}()

	go func() {
		for {
			time.Sleep(5 * time.Second)

			data := queue.DequeueAll()
			if len(data) > 0 {
				uniqueEntries := make(map[string]HistoryData)

				// Iterate through the data and add unique entries to the map
				for _, entry := range data {
					key := entry.SensorSerial + entry.FormattedTime
					uniqueEntries[key] = entry
				}

				// Convert the unique entries back to a slice
				uniqueData := make([]HistoryData, 0, len(uniqueEntries))
				for _, value := range uniqueEntries {
					uniqueData = append(uniqueData, value)
				}

				// Insert unique data to the database
				err := insertBatch(db, uniqueData)
				if err != nil {
					fmt.Println("Error:", err)
				}
			}
		}
	}()

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

// func sendEmail(attachmentPath string) error {
// 	from := "ijoon.helper@gmail.com"
// 	to := []string{"yot132@ijoon.net"}
// 	subject := "File Attachment"
// 	body := "Please find the attached file."

// 	mailer := gomail.NewMessage()
// 	mailer.SetHeader("From", from)
// 	mailer.SetHeader("To", to...)
// 	mailer.SetHeader("Subject", subject)
// 	mailer.SetBody("text/plain", body)

// 	attachmentName := filepath.Base(attachmentPath)
// 	mailer.Attach(attachmentPath, gomail.Rename(attachmentName))

// 	smtpHost := "smtp.gmail.com"
// 	smtpPort := 587
// 	smtpUsername := "ijoon.helper@gmail.com"
// 	smtpPassword := "ycbygboiryotnjyn"

// 	dialer := gomail.NewDialer(smtpHost, smtpPort, smtpUsername, smtpPassword)

// 	dialer.TLSConfig = nil

// 	if err := dialer.DialAndSend(mailer); err != nil {
// 		return err
// 	}

// 	return nil
// }

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
	} else {
		// log.Println("Image saved:", filePath)
	}
}

func publishStatus(data []byte, settop_serial, mac, sensor_serial string) error {
	broker_mutex.Lock()
	defer broker_mutex.Unlock()

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
		// for _, warning_temp := range thresholds[i].GetTempWarning() {
		if float64(max_temp) > tempWarningFloat {
			mImageStatus[sensor_serial] = "warning"
			// log.Println("tempWarningFloat = ", tempWarningFloat)
			// break
		}
		// }
		// for _, danger_temp := range thresholds[i].GetTempDanger() {
		if float64(max_temp) > tempDangerFloat {
			mImageStatus[sensor_serial] = "danger"
			// log.Println("tempDangerFloat = ", tempDangerFloat)

			// break
		}
		// }
	}

	// log.Println(sensor_serial, "   ", "event_end_time updated to:", mEventEndTimes[sensor_serial]+int64(EVENT_DELAY), ", ", time.Now().Unix(), "mImageStatus[sensor_serial] = ", mImageStatus[sensor_serial])
	if mEventEndTimes[sensor_serial]+int64(EVENT_DELAY) > time.Now().Unix() { // 기존 이벤트 유지시간
		if mImageStatus[sensor_serial] != "normal" {
			if mStatus[sensor_serial] != mImageStatus[sensor_serial] {
				mStatus[sensor_serial] = mImageStatus[sensor_serial]
				mEventEndTimes[sensor_serial] = time.Now().Add(EVENT_DELAY).Unix()
				var settop_uuid string
				var group_uuid string

				if mGroupUUIDs[sensor_serial] == nil {
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
				}

				j_frame := map[string]interface{}{
					"status":     mImageStatus[sensor_serial],
					"group_uuid": mGroupUUIDs[sensor_serial],
				}
				frameJSON, _ := json.Marshal(j_frame)
				set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
				client.Publish(set_topic, 1, false, frameJSON)

				// go func() {
				// 	_ = pub_token.Wait()
				// 	if pub_token.Error() != nil {
				// 		log.Println(pub_token.Error())
				// 	}
				// }()
			} else { // alarm delay is added
				mEventEndTimes[sensor_serial] = time.Now().Add(EVENT_DELAY).Unix()
			}
		}
	} else { // publish new event or event expired(change to "normal")
		switch mImageStatus[sensor_serial] {
		case "normal":
			if mStatus[sensor_serial] != "normal" && mEventEndTimes[sensor_serial] > 11 {
				mStatus[sensor_serial] = "normal"
				// mEventEndTimes = make(map[string]int64)
				var settop_uuid string
				var group_uuid string
				if mGroupUUIDs[sensor_serial] == nil {
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
				}

				j_frame := map[string]interface{}{
					"status":     mImageStatus[sensor_serial],
					"group_uuid": mGroupUUIDs[sensor_serial],
				}
				frameJSON, _ := json.Marshal(j_frame)
				set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
				client.Publish(set_topic, 1, false, frameJSON)
				// go func() {

				// 	_ = pub_token.Wait()
				// 	if pub_token.Error() != nil {
				// 		log.Println(pub_token.Error())
				// 	}

				// }()
			}
			break
		case "warning":
			fallthrough
		case "danger":
			mStatus[sensor_serial] = mImageStatus[sensor_serial]
			mEventEndTimes[sensor_serial] = time.Now().Add(EVENT_DELAY).Unix()

			var settop_uuid string
			var group_uuid string

			if mGroupUUIDs[sensor_serial] == nil {
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

			}
			j_frame := map[string]interface{}{
				"status":     mImageStatus[sensor_serial],
				"group_uuid": mGroupUUIDs[sensor_serial],
			}
			frameJSON, _ := json.Marshal(j_frame)
			set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
			client.Publish(set_topic, 1, false, frameJSON)

			// go func() {
			// 	log.Println("333333")
			// 	_ = pub_token.Wait()
			// 	if pub_token.Error() != nil {
			// 		log.Println(pub_token.Error())
			// 	}

			// }()

			break
		}
	}
	var minValue, maxValue float64
	currentTime := time.Now()
	formattedTime := currentTime.Format("2006-01-02 15:04:05")

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

	historyData := HistoryData{
		SensorSerial:  sensor_serial,
		MinValue:      minValue,
		MaxValue:      maxValue,
		FormattedTime: formattedTime,
	}
	queue.Enqueue(historyData)

	// query := fmt.Sprintf(`
	// 	INSERT INTO %s SET
	// 		min_temp = '%f',
	// 		max_temp = '%f',
	// 		date = '%s'
	// 	`,
	// 	sensor_serial, minValue, maxValue, formattedTime)

	// sqlAddRegisterer, err := db.Query(query)
	// if err != nil {
	// 	log.Println(err)
	// }
	// defer sqlAddRegisterer.Close()
	// broker_mutex.Unlock()

	return nil
}

func insertBatch(db *sql.DB, data []HistoryData) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	sortDataByTime(data)

	for _, sensorData := range data {
		query := fmt.Sprintf(`
			INSERT INTO %s (min_temp, max_temp, date)
			VALUES (?, ?, ?)
		`, sensorData.SensorSerial)

		stmt, err := tx.Prepare(query)
		if err != nil {
			return err
		}

		_, err = stmt.Exec(sensorData.MinValue, sensorData.MaxValue, sensorData.FormattedTime)
		stmt.Close()

		if err != nil {
			return err
		}
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func sortDataByTime(data []HistoryData) {
	// time.Time 형태로 변환
	timeData := make([]struct {
		time.Time
		Index int
	}, len(data))

	for i, d := range data {
		parsedTime, _ := time.Parse("2006-01-02 15:04:05", d.FormattedTime)
		timeData[i] = struct {
			time.Time
			Index int
		}{parsedTime, i}
	}

	// 정렬
	sort.Slice(timeData, func(i, j int) bool {
		return timeData[i].Time.Before(timeData[j].Time)
	})

	// 정렬된 순서로 데이터 갱신
	sortedData := make([]HistoryData, len(data))
	for i, td := range timeData {
		sortedData[i] = data[td.Index]
	}

	// 원본 데이터 갱신
	copy(data, sortedData)
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
func saveImageToFile(filePath string, data []byte, settop_serial, mac, sensor_serial string) error {
	j_frame := map[string]interface{}{}
	err := json.Unmarshal(data, &j_frame)
	if err != nil {
		log.Println(err)
		return err
	}

	imgData, _ := base64.StdEncoding.DecodeString(j_frame["raw"].(string))

	go publishStatus(imgData, settop_serial, mac, sensor_serial)

	err = saveRawToFile(filePath, imgData)
	if err != nil {
		log.Println("Error saving raw data to file:", err)
		return nil
	}
	return nil
}

func saveRawToFile(filePath string, rawData []byte) error {
	file, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(rawData)
	if err != nil {
		return err
	}

	return nil
}

func createFolder(folderPath string) error {
	// 폴더가 존재하지 않으면 생성
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

func deleteOldImages(basePath string) {
	ticker := time.NewTicker(7 * 24 * time.Hour) // 24시간마다 실행
	// ticker := time.NewTicker(10 * time.Second) //test

	for {
		select {
		case <-ticker.C:
			// history 삭제 (7주일 보관)==
			deleteOldDataWithTransaction(db)
			folders, err := getFolders(basePath)
			if err != nil {
				log.Println("Error getting folders:", err)
				continue
			}

			for _, folder := range folders {
				deleteOldImagesInFolder(filepath.Join(basePath, folder))
			}
		}
	}
}

func getFolders(basePath string) ([]string, error) {
	var folders []string
	dirs, err := ioutil.ReadDir(basePath)
	if err != nil {
		return nil, err
	}

	for _, dir := range dirs {
		if dir.IsDir() {
			folders = append(folders, dir.Name())
		}
	}

	return folders, nil
}

func deleteOldDataWithTransaction(db *sql.DB) error {
	sevenDaysAgo := time.Now().AddDate(0, 0, -7)
	formattedSevenDaysAgo := sevenDaysAgo.Format("2006-01-02 15:04:05")

	tx, err := db.Begin()
	if err != nil {
		return err
	}

	deleteQuery := fmt.Sprintf(`
			DELETE FROM history
			WHERE date < '%s'
		`, formattedSevenDaysAgo)

	_, err = tx.Exec(deleteQuery)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}

	return nil
}

func deleteOldImagesInFolder(folderPath string) {
	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if info.ModTime().Before(time.Now().Add(-8 * 24 * time.Hour)) {
				// if info.ModTime().Before(time.Now().Add(10 * time.Second)) {
				err := os.Remove(path)
				if err != nil {
					fmt.Println("Error deleting file:", err)
				} else {
					fmt.Println("Deleted file:", path)
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Println("Error deleting old images in folder:", err)
	}

	err = os.RemoveAll(folderPath)
	if err != nil {
		fmt.Println("Error deleting folder:", err)
	} else {
		fmt.Println("Deleted folder:", folderPath)
	}
}

func SendMessageHandler(title string, body string, style string, topic string) {
	firebaseutil.SendMessageAsync(title, body, style, topic)

}

func eventMessage(settop_serial string, sensor_type string, level_temp int, sensor_serial string) string {
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

	var lev string
	if level_temp == 1 {
		lev = "이상징후 (주의)"
		serverLog("이상징후가 발견되었습니다. (주의)", "anomaly", sensor_serial)
	} else if level_temp == 2 {
		lev = "이상징후 (위험)"
		serverLog("이상징후가 발견되었습니다. (위험)", "anomaly", sensor_serial)
	} else if level_temp == 3 {
		lev = "이상징후 (점검)"
		serverLog("이상징후가 발견되었습니다. (점검)", "anomaly", sensor_serial)
	}
	var checkEmptyPlace string
	if place_name == "" && floor == "" {
		checkEmptyPlace = ""
	} else {
		checkEmptyPlace = place_name + " " + floor + "에 설치된 "
	}
	message = msg_formattedTime + " " + checkEmptyPlace + room + " " + sensor_type + " 센서에서 " + lev + "가 발견되었습니다. 해당 위치를 확인하시기 바랍니다."
	if level_temp == 0 {
		message = msg_formattedTime + " " + checkEmptyPlace + room + " " + sensor_type + " 센서가 정상입니다."
	}
	return message
}

func serverLog(message string, unit string, sensor_serial string) {
	var uuid = uuid.New()

	formattedTime := time.Now().Format("2006-01-02 15:04:05")

	query := fmt.Sprintf(`
		INSERT INTO log SET
			uuid = '%s', 
			unit = '%s',
			message = '%s',
			sensor_serial = '%s',
			registered_time = '%s'
		`,
		uuid.String(), unit, message, sensor_serial, formattedTime)

	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}
	defer sqlAddRegisterer.Close()
}

func subscribeHandler(client mqtt.Client, topic string) {
	mu.Lock()
	defer mu.Unlock()
	if token := client.Subscribe(topic, 0, func(client mqtt.Client, msg mqtt.Message) {
		payloadStr := string(msg.Payload())
		parts := strings.Split(msg.Topic(), "/")
		logger.Title.Printf(topic)

		switch {
		case strings.HasPrefix(topic, base_topic+"/get/settop_sn/"):
			handleGetSettop_SN(client, parts, payloadStr)

		case strings.HasPrefix(topic, base_topic+"/regist/sensor/"):
			handleRegistSensor(client, parts, payloadStr)

		case strings.HasPrefix(topic, base_topic+"/deregist/sensor/"):
			handleDeregistSensor(client, parts)

		// case strings.HasPrefix(topic, base_topic+"/data/threshold9/"):
		// 	handleThreshold9Data(client, parts, msg.Payload())

		case strings.HasPrefix(topic, base_topic+"/data/frame/"):
			handleFrameData(client, parts, msg.Payload(), msg)

		case strings.HasPrefix(topic, base_topic+"/data/connection/"):
			handleConnectionData(client, parts, payloadStr)

		case strings.HasPrefix(topic, base_topic+"/data/status/"):
			handleStatusData(client, parts, payloadStr, msg)

		case strings.HasPrefix(topic, base_topic+"/data/info/"):
			handleInfoData(client, parts, payloadStr, msg)
		}
	}); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
	}
}

func handleGetSettop_SN(client mqtt.Client, parts []string, payloadStr string) {
	broker_mutex.Lock()
	defer broker_mutex.Unlock()

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
	client.Publish(set_topic, 1, false, message)

	// go func() {
	// 	_ = pub_token.Wait()
	// 	if pub_token.Error() != nil {
	// 		log.Println(pub_token.Error())
	// 	}
	// }()
	// broker_mutex.Unlock()
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

func handleRegistSensor(client mqtt.Client, parts []string, payloadStr string) {
	broker_mutex.Lock()
	defer broker_mutex.Unlock()

	currentTime := time.Now()
	formattedTime := currentTime.Format("2006-01-02 15:04:05")
	var settop_uuid string
	var uuid = uuid.New()
	settop_serial := parts[3]
	settopMac := parts[4]
	sensorSerial := parts[5]
	createSensorDataTable(db, sensorSerial)
	query := fmt.Sprintf(`
		SELECT uuid 
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
		err := rows.Scan(&settop_uuid)
		if err != nil {
			log.Println(err)
		}
	}
	if settop_uuid == "" {
		log.Println("settop_uuid not found")
		return
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
	mainListMapping = NewMainListResponseMapping()
	initThreshold9Data(uuid.String())
	defer sqlAddSensor.Close()
	serverLog("센서가 서버에 연결되었습니다.", "web", sensorSerial)

	set_topic := base_topic + "/get/info/" + settop_serial + "/" + settopMac
	message := "info"
	client.Publish(set_topic, 1, false, message)

	// go func() {
	// 	_ = pub_token.Wait()
	// 	if pub_token.Error() != nil {
	// 		log.Println(pub_token.Error())
	// 	}
	// }()
	// broker_mutex.Unlock()
}

func handleDeregistSensor(client mqtt.Client, parts []string) {
	broker_mutex.Lock()
	defer broker_mutex.Unlock()

	sensorSerial := parts[5]
	log.Println("handleDeregistSensor called: ", sensorSerial)
	query := fmt.Sprintf(`
		UPDATE sensor SET
			status = '%s'
		WHERE serial = '%s'
		`,
		"3", sensorSerial)

	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
	}
	mainListMapping = NewMainListResponseMapping()
	serverLog("센서의 연결이 끊어졌습니다.", "web", sensorSerial)
	// broker_mutex.Unlock()
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

func handleFrameData(client mqtt.Client, parts []string, payload []byte, msg mqtt.Message) {
	broker_mutex.Lock()
	defer broker_mutex.Unlock()

	basePath := "storage_data/"
	processMqttMessage(msg, basePath)
	var decodedData map[string]interface{}
	err := json.Unmarshal(msg.Payload(), &decodedData)
	// broker_mutex.Unlock()
	if err != nil {
		serverLog("센서에서 전송된 Packet에 오류가 발견되었습니다. (Code.E01)", "web", parts[5])
		fmt.Println("JSON decoding error:", err)
		return
	}
}

// var aliveSensorLastAliveTime = make(map[string]time.Time)

func handleConnectionData(client mqtt.Client, parts []string, payloadStr string) {

	settop_serial := parts[3]
	mac := parts[4]
	query := fmt.Sprintf(`
		UPDATE settop SET
			is_alive = '%s'
		WHERE serial = '%s'
		`,
		payloadStr, settop_serial)
	_, err := db.Exec(query)
	if err != nil {
		log.Println(err)
	}
	// log.Println("handleConnectionData called: ", settop_serial, ", ", payloadStr)

	// query = fmt.Sprintf(`
	// 			UPDATE sensor SET
	// 				status = '%s'
	// 			`,
	// 	"0")
	// _, err = db.Exec(query)
	// if err != nil {
	// 	log.Println(err)
	// }

	if payloadStr == "0" {
		broker_mutex.Lock()
		defer broker_mutex.Unlock()
		log.Println("e1: ", mac)

		query = fmt.Sprintf(`
				UPDATE sensor SET
					status = '%s'
				WHERE mac = '%s'
				`,
			"3", mac)
		_, err = db.Exec(query)
		if err != nil {
			log.Println(err)
		}
	}
	// else {
	// 	aliveSensorLastAliveTime[mac] = time.Now()
	// }
	// go checkAndUpdateDeadSensors()
	_, err = db.Exec(query)
	if err != nil {
		log.Println(err)
	}

	mainListMapping = NewMainListResponseMapping()
	// broker_mutex.Unlock()
}

// func checkAndUpdateDeadSensors() {
// 	mu.Lock()
// 	defer mu.Unlock()

// 	currentTime := time.Now()

// 	for mac, lastAliveTime := range aliveSensorLastAliveTime {
// 		// log.Println("1 = ", currentTime.Sub(lastAliveTime), ", 2 = ", 10*time.Second)
// 		if currentTime.Sub(lastAliveTime) > 1*time.Minute {
// 			delete(aliveSensorLastAliveTime, mac)
// 			// The last alive time is more than 1 minute ago, update the database
// 			query := fmt.Sprintf(`
//                 UPDATE sensor SET
//                     status = '%s'
//                 WHERE mac = '%s'
//             `, "3", mac)

// 			_, err := db.Exec(query)
// 			if err != nil {
// 				log.Println(err)
// 			}
// 		}
// 	}
// }

func handleStatusData(client mqtt.Client, parts []string, payloadStr string, msg mqtt.Message) {
	// mu.Lock()
	// defer mu.Unlock()

	currentTime := time.Now()
	formattedTime := currentTime.Format("2006-01-02 15:04:05")
	var uuid = uuid.New()
	var sensor_type sql.NullString
	settop_serial := parts[3]
	sensor_serial := parts[5]
	mac := parts[4]
	query := fmt.Sprintf(`
		SELECT type 
		FROM sensor 
		WHERE serial = '%s'
	`,
		sensor_serial)

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
	}

	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&sensor_type)
		if err != nil {
			log.Println(err)
		}
	}

	j_frame := map[string]interface{}{}
	err = json.Unmarshal(msg.Payload(), &j_frame)
	var status string
	if err != nil { //sensor publish <<=
		status = string(msg.Payload())
		if status == "inspection" {
			log.Println("inspection called")
			var settop_uuid string
			var group_uuid string

			if mGroupUUIDs[sensor_serial] == nil {
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
			}
			set_topic := base_topic + "/data/status/" + settop_serial + "/" + mac + "/" + sensor_serial
			j_frame := map[string]interface{}{
				"status":     "inspection",
				"group_uuid": mGroupUUIDs[sensor_serial],
			}
			frameJSON, _ := json.Marshal(j_frame)
			client.Publish(set_topic, 1, false, frameJSON)

			// go func() {

			// 	_ = pub_token.Wait()
			// 	if pub_token.Error() != nil {
			// 		log.Println(pub_token.Error())
			// 	}

			// }()
		}
	} else {
		status, _ = j_frame["status"].(string)
	}

	var result = -1
	if string(status) == "normal" {
		result = 0
	} else if string(status) == "warning" {
		result = 1
	} else if string(status) == "danger" {
		result = 2
	} else if string(status) == "inspection" {
		result = 3
	}
	if result >= 0 {
		message := eventMessage(settop_serial, getNullStringValidValue(sensor_type), result, sensor_serial)
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
				SendMessageHandler("Trusafer", message, "style", group_uuid)
			}
		}
		var group_master_uuid string
		query := fmt.Sprintf(`
			SELECT uuid 
			FROM group_ 
			WHERE name = 'master'
		`)

		rows, err := db.Query(query)
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
		SendMessageHandler("Trusafer", message, "style", group_master_uuid)

		query = fmt.Sprintf(`
			INSERT INTO log SET
				uuid = '%s', 
				unit = '%s',
				message = '%s',
				sensor_serial = '%s',
				registered_time = '%s'
			`,
			uuid.String(), "mobile", message, sensor_serial, formattedTime)

		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
		}
		defer sqlAddRegisterer.Close()

		sensorSerial := parts[5]
		query = fmt.Sprintf(`
			UPDATE sensor SET
				status = '%s'
			WHERE serial = '%s'
			`,
			strconv.Itoa(result), sensorSerial)

		_, err = db.Exec(query)
		if err != nil {
			log.Println(err)
		}
		mainListMapping = NewMainListResponseMapping()

	}
	// broker_mutex.Unlock()

}

func handleInfoData(client mqtt.Client, parts []string, payloadStr string, msg mqtt.Message) {
	broker_mutex.Lock()
	defer broker_mutex.Unlock()

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
	// broker_mutex.Unlock()
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

			// find global min/max
			if temp < min {
				min = temp
			}
			if temp > max {
				max = temp
			}

			// find 9 area min/max
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
