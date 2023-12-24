package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
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
	"time"

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

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var saveImageDir string
var client mqtt.Client
var isPublished bool

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

// func uploadFile(w http.ResponseWriter, r *http.Request) {
// 	file, handler, err := r.FormFile("file")
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	defer file.Close()

// 	f, err := os.OpenFile(handler.Filename, os.O_WRONLY|os.O_CREATE, 0666)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	defer f.Close()

// 	io.Copy(f, file)

// 	fmt.Fprintf(w, "File uploaded successfully")
// }

func accessibleRolesForRT() map[string][]string {
	return map[string][]string{
		"admin": {"refresh"},
		"user":  {"refresh"},
	}
}

// firebase init
func initApp() {
	serviceAccountKeyPath := "./serviceAccountKey.json"
	_, err := firebaseutil.InitApp(serviceAccountKeyPath)
	if err != nil {
		log.Fatalln("[initApp] initializing app error :", err)
	}
}

func main() {
	initApp()
	conf_file := flag.String("config", "/Users/bkpark/works/go/trusafer/main_server/config.json", "config file path")
	flag.Parse()
	conf, err := LoadConfiguration(*conf_file)
	if err != nil {
		log.Fatal(err)
	}
	dbID := conf.Database.Id
	dbPW := conf.Database.Password
	dbAddr := conf.Database.Address
	dbName := conf.Database.Name
	grpcPort := conf.Grpc.Port
	grpcCert := conf.Grpc.CertFile
	grpcKey := conf.Grpc.KeyFile
	saveImageDir = conf.Etc.SaveImageDir
	gwPort := conf.Gw.Port
	// secret_key_rt := conf.Jwt.SecretKeyRT
	secret_key_at := conf.Jwt.SecretKeyAT
	token_duration_at := conf.Jwt.TokenDurationAT
	// token_duration_rt := conf.Jwt.TokenDurationRT

	broker := "ssl://192.168.13.5:21984"
	// mqtt_serial := "serial0021231321"
	mqtt_serial := RandomString(16)
	username := "ijoon"
	password := "9DGQhyCH6RZ4"
	topic := "trusafer"
	opts1 := mqtt.NewClientOptions().AddBroker(broker)

	tlsconfig := NewTLSConfig()

	opts1.SetClientID(mqtt_serial).SetTLSConfig(tlsconfig)
	opts1.SetUsername(username)
	opts1.SetPassword(password)
	opts1.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		println("mqtt connection lost error: " + err.Error())
	})
	opts1.SetReconnectingHandler(func(c mqtt.Client, options *mqtt.ClientOptions) {
		println("mqtt reconnecting...")
	})

	opts1.SetOnConnectHandler(func(c mqtt.Client) {
		println("mqtt connected!")
		currentTime := time.Now()
		formattedTime := currentTime.Format("2006-01-02 15:04:05")
		var settop_uuid string
		var settop_serial string

		//===================================================================================================//

		if token := client.Subscribe(topic+"/#", 2, func(client mqtt.Client, msg mqtt.Message) {
			parts := strings.Split(msg.Topic(), "/")
			if len(parts) > 1 {
				mac := parts[1]
				mac_parts := strings.Split(mac, ":")

				if len(mac_parts) > 5 {
					if !isPublished {
						isPublished = true

						//mac address (ip_module)
						query := fmt.Sprintf(`
							SELECT serial 
							FROM settop 
							WHERE mac1 = '%s' OR mac2 = '%s'
						`,
							mac, mac)

						rows, err := db.Query(query)
						if err != nil {
							log.Println(err)
							err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
						}

						defer rows.Close()
						for rows.Next() {
							err := rows.Scan(&settop_serial)
							if err != nil {
								log.Println(err)
								err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
							}
						}

						if settop_serial == "" {
							log.Println("err settop_serial")
							return
						}
						if mac == "" {
							log.Println("err mac empty")
							return
						}

						set_topic := "trusafer/" + mac + "/settop_sn/data"

						message := settop_serial

						log.Println(settop_serial)
						pub_token := client.Publish(set_topic, 0, false, message)

						go func() {
							_ = pub_token.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
							if pub_token.Error() != nil {
								log.Println(pub_token.Error()) // Use your preferred logging technique (or just fmt.Printf)
							}
							time.Sleep(10 * time.Second)
							isPublished = false

						}()

					}
				}
			}

			// register
			if len(parts) > 4 {
				var uuid = uuid.New()

				if parts[4] == "regist" {
					// settopSerial := parts[1]
					settopMac := parts[2]
					sensorSerial := parts[3]
					query := fmt.Sprintf(`
						SELECT uuid 
						FROM settop 
						WHERE mac1 = '%s' OR mac2 = '%s'
					`,
						settopMac, settopMac)
					rows, err := db.Query(query)
					if err != nil {
						log.Println(err)
						err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
					}

					defer rows.Close()
					for rows.Next() {
						err := rows.Scan(&settop_uuid)
						if err != nil {
							log.Println(err)
							err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
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
								status = '%d',
								serial = '%s',
								ip_address = '%s',
								location = '%s',
								latest_version = '%s',
								registered_time = '%s',
								mac = '%s'
						ON DUPLICATE KEY UPDATE
							settop_uuid = VALUES(settop_uuid),
							status = VALUES(status),
							ip_address = VALUES(ip_address),
							location = VALUES(location),
							latest_version = VALUES(latest_version),
							registered_time = VALUES(registered_time),
							mac = VALUES(mac)
					`,
						uuid.String(), settop_uuid, 0,
						sensorSerial, "", "", "",
						formattedTime, settopMac,
					)

					sqlAddSensor, err := db.Query(query)

					if err != nil {
						log.Println(err)
					}
					mainListMapping = NewMainListResponseMapping()

					threshold_topic := "trusafer/" + parts[1] + "/" + parts[2] + "/" + parts[3] + "/threshold9/get"
					pub_token := client.Publish(threshold_topic, 0, false, "")

					go func() {
						_ = pub_token.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
						if pub_token.Error() != nil {
							log.Println(pub_token.Error()) // Use your preferred logging technique (or just fmt.Printf)
						}
						time.Sleep(1 * time.Second)
						isPublished = false
					}()
					defer sqlAddSensor.Close()
					serverLog("센서가 서버에 연결되었습니다.", "web", sensorSerial)
				}
				if parts[4] == "deregist" {
					sensorSerial := parts[3]
					query := fmt.Sprintf(`
					UPDATE sensor SET
						status = '%d'
					WHERE serial = '%s'
					`,
						3, sensorSerial)

					_, err := db.Exec(query)
					if err != nil {
						log.Println(err)
						// gRPC 오류를 생성하여 상태 코드 설정
					}
					mainListMapping = NewMainListResponseMapping()
					serverLog("센서의 연결이 끊어졌습니다.", "web", sensorSerial)
				}
				// history data
				if parts[4] == "data" {
					sensorSerial := parts[3]
					basePath := "storage_data/"
					processMqttMessage(msg, basePath)
					var decodedData map[string]interface{}
					err := json.Unmarshal(msg.Payload(), &decodedData)
					if err != nil {
						serverLog("센서에서 전송된 Packet에 오류가 발견되었습니다. (Code.E01)", "web", parts[3])
						fmt.Println("JSON decoding error:", err)
						return
					}
					var minValue, maxValue float64

					maxValues, ok := decodedData["max"].([]interface{})
					if !ok {
						// 적절한 타입으로 변환할 수 없는 경우 처리
						fmt.Println("Error: 'max' is not of type []float64")
						return
					}

					for i, value := range maxValues {
						if intValue, ok := value.(float64); ok {
							if i == 0 {
								maxValue = intValue
							} else {
								maxValue = math.Max(maxValue, float64(intValue))
							}
						}
					}
					minValues, ok := decodedData["min"].([]interface{})
					if !ok {
						fmt.Println("Error: 'min' is not of type []float64")
						return
					}

					for i, value := range minValues {
						if intValue, ok := value.(float64); ok {
							if i == 0 {
								minValue = intValue
							} else {
								minValue = math.Min(minValue, float64(intValue))
							}
						}
					}
					formattedTime := time.Now().Format("2006-01-02 15:04:05")
					// log.Println(formattedTime)
					query := fmt.Sprintf(`
						INSERT INTO history SET
							uuid = '%s', 
							sensor_serial = '%s',
							min_temp = '%f',
							max_temp = '%f',
							date = '%s'
						`,
						uuid.String(), sensorSerial, minValue, maxValue, formattedTime)

					sqlAddRegisterer, err := db.Query(query)
					if err != nil {
						log.Println(err)
						err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
					}
					defer sqlAddRegisterer.Close()
				}

				if parts[3] == "connection" {
					// var decodedData map[string]interface{}
					// err := json.Unmarshal(msg.Payload(), &decodedData)
					// if err != nil {
					// 	fmt.Println("JSON decoding error:", err)
					// 	return
					// }
					payloadStr := string(msg.Payload())
					isAlive, err := strconv.Atoi(payloadStr)

					settop_serial := parts[1]
					query := fmt.Sprintf(`
						UPDATE settop SET
							is_alive = '%d'
						WHERE serial = '%s'
						`,
						isAlive, settop_serial)

					_, err = db.Exec(query)
					if err != nil {
						log.Println(err)
					}
					mainListMapping = NewMainListResponseMapping()

				}

				if parts[4] == "event" {
					var sensorData SensorData
					var sensor_type string
					var check_serial string
					query := fmt.Sprintf(`
							SELECT serial 
							FROM settop 
							WHERE mac1 = '%s'
						`,
						parts[2])

					rows, err := db.Query(query)
					if err != nil {
						log.Println(err)
						err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
					}

					defer rows.Close()
					for rows.Next() {
						err := rows.Scan(&check_serial)
						if err != nil {
							log.Println(err)
							err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
						}
					}

					if check_serial != "" {
						sensor_type = "A"
					} else {
						sensor_type = "B"
					}

					err = json.Unmarshal([]byte(msg.Payload()), &sensorData)
					if err != nil {
						fmt.Println("JSON Unmarshal error:", err)
						return
					}
					result := checkSensorData(sensorData)
					formattedTime := time.Now().Format("2006-01-02 15:04:05")

					message := eventMessage(parts[1], sensor_type, result, parts[3])
					SendMessageHandler("Trusafer", message, "style", "default")

					query = fmt.Sprintf(`
						INSERT INTO log SET
							uuid = '%s', 
							unit = '%s',
							message = '%s',
							sensor_serial = '%s',
							registered_time = '%s'
						`,
						uuid.String(), "mobile", message, parts[3], formattedTime)

					sqlAddRegisterer, err := db.Query(query)
					if err != nil {
						log.Println(err)
					}
					defer sqlAddRegisterer.Close()

					sensorSerial := parts[3]
					query = fmt.Sprintf(`
					UPDATE sensor SET
						status = '%d'
					WHERE serial = '%s'
					`,
						result, sensorSerial)

					_, err = db.Exec(query)
					if err != nil {
						log.Println(err)
					}
					mainListMapping = NewMainListResponseMapping()
				}

				if len(parts) == 6 {
					if parts[5] == "data" {
						var tempInit TempInit

						var sensor_uuid string
						query := fmt.Sprintf(`
							SELECT uuid  
							FROM sensor 
							WHERE serial = '%s' 
						`,
							parts[3])

						rows, err := db.Query(query)
						if err != nil {
							log.Println(err)
						}

						defer rows.Close()
						for rows.Next() {
							err := rows.Scan(&sensor_uuid)
							if err != nil {
								log.Println(err)
							}
						}

						err = json.Unmarshal([]byte(msg.Payload()), &tempInit)
						if err != nil {
							fmt.Println("JSON Unmarshal error:", err)
							return
						}
						query = fmt.Sprintf(`
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
							sensor_uuid, strconv.Itoa(tempInit.Warning[0]), strconv.Itoa(tempInit.Danger[0]),
							strconv.Itoa(tempInit.Warning[1]), strconv.Itoa(tempInit.Danger[1]),
							strconv.Itoa(tempInit.Warning[2]), strconv.Itoa(tempInit.Danger[2]),
							strconv.Itoa(tempInit.Warning[3]), strconv.Itoa(tempInit.Danger[3]),
							strconv.Itoa(tempInit.Warning[4]), strconv.Itoa(tempInit.Danger[4]),
							strconv.Itoa(tempInit.Warning[5]), strconv.Itoa(tempInit.Danger[5]),
							strconv.Itoa(tempInit.Warning[6]), strconv.Itoa(tempInit.Danger[6]),
							strconv.Itoa(tempInit.Warning[7]), strconv.Itoa(tempInit.Danger[7]),
							strconv.Itoa(tempInit.Warning[8]), strconv.Itoa(tempInit.Danger[8]))
						sqlThresh, err := db.Query(query)
						if err != nil {
							log.Println(err)
						}
						defer sqlThresh.Close()
					}
				}
			}

		}); token.Wait() && token.Error() != nil {
			print(token.Error())
		}

		//===================================================================================================//
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

	// 액세스 토큰을 위한 JWTManager 및 AuthInterceptor
	jwtManagerAT := service.NewJWTManager(secret_key_at, token_duration_at)
	interceptorAT := service.NewAuthInterceptor(jwtManagerAT, accessibleRolesForRT())
	opts = append(opts, grpc.Creds(creds))
	opts = append(opts, grpc.UnaryInterceptor(interceptorAT.Unary()))

	grpcServer := grpc.NewServer()

	// gRPC 서버 네트워크 연결 대기
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	pb.RegisterMainControlServer(grpcServer, &server{})

	// log.Printf("start gRPC server on %d port", grpcPort)
	// if err := grpcServer.Serve(lis); err != nil {
	// 	log.Fatalf("failed to serve: %s", err)
	// }

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

	// go func() {
	log.Printf("Serving gRPC-Gateway on " + gwPortString + " port")
	log.Fatalln(gwServer.ListenAndServeTLS(certFile, keyFile))
	// }()
	// http.HandleFunc("/upload", handleUpload)
	// httpServer := &http.Server{
	// 	Addr:    ":8080",
	// 	Handler: nil, // Use the default ServeMux
	// }

	// log.Printf("Serving REST API on :8080 port")
	// log.Fatalln(httpServer.ListenAndServe())

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

// func handleUpload(w http.ResponseWriter, r *http.Request) {
// 	// Parse the Multipart/form-data request
// 	err := r.ParseMultipartForm(10 << 20) // 10 MB limit for the entire request
// 	if err != nil {
// 		http.Error(w, "Error parsing form", http.StatusBadRequest)
// 		return
// 	}

// 	// Extract Protobuf message from the form field
// 	protobufData, _, err := r.FormFile("protobuf_data")
// 	if err != nil {
// 		http.Error(w, "Error retrieving protobuf data", http.StatusBadRequest)
// 		return
// 	}
// 	defer protobufData.Close()

// 	protobufBytes, err := ioutil.ReadAll(protobufData)
// 	if err != nil {
// 		http.Error(w, "Error reading protobuf data", http.StatusInternalServerError)
// 		return
// 	}

// 	registerer := &pb.Registerer{}
// 	err = proto.Unmarshal(protobufBytes, registerer)
// 	if err != nil {
// 		http.Error(w, "Error unmarshalling protobuf data", http.StatusInternalServerError)
// 		return
// 	}

// 	// Extract the file from the form field
// 	file, fileHeader, err := r.FormFile("company_number_file")
// 	if err != nil {
// 		http.Error(w, "Error retrieving file", http.StatusBadRequest)
// 		return
// 	}
// 	defer file.Close()
// 	originalFileName := fileHeader.Filename

// 	fileBytes, err := ioutil.ReadAll(file)
// 	savePath := filepath.Join("/Users", "bkpark", "trusafer")

// 	err1 := os.MkdirAll(savePath, 0755)
// 	if err1 != nil {
// 		log.Printf("Error creating directory: %v", err)
// 	}

// 	filePath := filepath.Join(savePath, originalFileName)

// 	err = ioutil.WriteFile(filePath, fileBytes, 0644)
// 	if err != nil {
// 		log.Printf("Error saving file: %v", err)
// 	}

// 	err = sendEmail(filePath)
// 	if err != nil {
// 		log.Printf("Error sending email: %v", err)
// 	}

// 	// Process the Protobuf message and file as needed
// 	fmt.Printf("Received Registerer:\n%+v\n", registerer)
// 	// fileBytes now contains the binary data of the file

// 	w.WriteHeader(http.StatusOK)
// 	w.Write([]byte("Upload successful"))
// }

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
	// Import trusted certificates from CAfile.pem.
	// Alternatively, manually add CA certificates to
	// default openssl CA bundle.
	certpool := x509.NewCertPool()
	pemCerts, err := ioutil.ReadFile("cert/rootca.crt")
	if err == nil {
		certpool.AppendCertsFromPEM(pemCerts)
	} else {
	}

	// Import client certificate/key pair
	// cert, err := tls.LoadX509KeyPair("cert/client-crt.pem", "cert/client-key.pem")
	cert, err := tls.LoadX509KeyPair("cert/server.crt", "cert/private.pem")
	if err != nil {
		panic(err)
	}

	// Just to print out the client certificate..
	cert.Leaf, err = x509.ParseCertificate(cert.Certificate[0])
	if err != nil {
		panic(err)
	}
	// fmt.Println(cert.Leaf)

	// Create tls.Config with desired tls properties
	return &tls.Config{
		// RootCAs = certs used to verify server cert.
		RootCAs: certpool,
		// ClientAuth = whether to request cert from server.
		// Since the server is set up for SSL, this happens
		// anyways.
		ClientAuth: tls.NoClientCert,
		// ClientCAs = certs used to validate client cert.
		ClientCAs: nil,
		// InsecureSkipVerify = verify that cert contents
		// match server. IP matches what is in cert etc.
		InsecureSkipVerify: true,
		// Certificates = list of certs client sends to server.
		Certificates: []tls.Certificate{cert},
	}
}

// func saveImagesPeriodically(client mqtt.Client, topics []string, basePath string) {
// 	ticker := time.NewTicker(10 * time.Second) // 10초마다 실행
// 	for {
// 		select {
// 		case <-ticker.C:
// 			for _, topic := range topics {
// 				token := client.Subscribe(topic, 2, func(client mqtt.Client, msg mqtt.Message) {
// 					processMqttMessage(msg, basePath)
// 				})
// 				if token.Wait() && token.Error() != nil {
// 					log.Println("Error subscribing to topic:", token.Error())
// 				}
// 			}
// 		}
// 	}
// }

func processMqttMessage(msg mqtt.Message, basePath string) {
	parts := strings.Split(msg.Topic(), "/")
	if len(parts) >= 3 {
		serial := parts[3]

		formattedDate := time.Now().Format("2006-01-02")
		folderPath := filepath.Join(basePath, serial, formattedDate)
		err := createFolder(folderPath)
		if err != nil {
			log.Println("Error creating folder:", err)
			return
		}

		formattedTime := time.Now().Format("15:04:05")
		fileName := formattedTime + ".jpg"
		filePath := filepath.Join(folderPath, fileName)

		err = saveImageToFile(filePath, msg.Payload(), serial)
		if err != nil {
			log.Println("Error saving image:", err)
		} else {
			// log.Println("Image saved:", filePath)
		}
	}
}

func saveImageToFile(filePath string, data []byte, sensor_serial string) error {
	j_frame := map[string]interface{}{}
	err := json.Unmarshal(data, &j_frame)
	if err != nil {
		log.Println(err)
	}
	b, err := base64.StdEncoding.DecodeString(j_frame["img"].(string))

	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(b)
	if err != nil {
		serverLog("센서에서 전송된 Packet에 오류가 발견되었습니다. (Code.E02)", "web", sensor_serial)
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
	ticker := time.NewTicker(8 * 24 * time.Hour) // 24시간마다 실행
	// ticker := time.NewTicker(10 * time.Second) // 3초마다 실행

	for {
		select {
		case <-ticker.C:
			// basePath 아래의 모든 폴더 검색
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

	// 폴더 삭제
	err = os.RemoveAll(folderPath)
	if err != nil {
		fmt.Println("Error deleting folder:", err)
	} else {
		fmt.Println("Deleted folder:", folderPath)
	}
}

func SendMessageHandler(title string, body string, style string, topic string) {
	firebaseutil.SendMessage(title, body, style, topic)
}

func checkSensorData(data SensorData) int {
	hasDanger := false
	hasWarning := false

	for _, value := range data.Danger {
		if value {
			hasDanger = true
			break
		}
	}

	for _, value := range data.Warning {
		if value {
			hasWarning = true
			break
		}
	}

	if hasDanger && hasWarning {
		return 2
	} else if hasWarning {
		return 1
	} else if hasDanger {
		return 2
	} else {
		return 0
	}
}

func eventMessage(settop_serial string, sensor_type string, level_temp int, sensor_serial string) string {
	var place_uuid string
	var room string
	var floor string
	var place_name string
	var message string
	msg_formattedTime := time.Now().Format("2006년 01월 02일 15시 04분 05초")

	log.Println("123 =", sensor_serial)
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
	} else {
		lev = "이상징후 (점검)"
		serverLog("이상징후가 발견되었습니다. (점검)", "anomaly", sensor_serial)
	}
	message = msg_formattedTime + " " + place_name + " " + floor + "에 설치된 " + room + " " + sensor_type + " 센서에서 " + lev + "가 발견되었습니다. 해당 위치를 확인하시기 바랍니다."
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
