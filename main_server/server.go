package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
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
	topic := "truwin/settop"
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
		var uuid = uuid.New()
		var settop_uuid string
		if token := client.Subscribe(topic+"/b/info", 2, func(client mqtt.Client, msg mqtt.Message) {
			var decodedData map[string]interface{}
			err := json.Unmarshal(msg.Payload(), &decodedData)
			if err != nil {
				fmt.Println("JSON decoding error:", err)
				return
			}

			serial := decodedData["SN"]
			mac := decodedData["mac"]
			point_mac := strings.ReplaceAll(mac.(string), "\n", "")

			query := fmt.Sprintf(`
				SELECT uuid 
				FROM settop 
				WHERE mac1 = '%s' OR mac2 = '%s'
			`,
				point_mac, point_mac)

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

			// log.Println("settop_uuid = ", settop_uuid)
			query = fmt.Sprintf(`
				INSERT IGNORE INTO sensor SET
					uuid = '%s', 
					settop_uuid = '%s',
					status = '%d',
					serial = '%s',
					ip_address = '%s',
					location = '%s',
					threshold_temp_warning = '%s',
					threshold_temp_danger = '%s',
					latest_version = '%s',
					registered_time = '%s',
					mac = '%s'
			`,
				uuid.String(), settop_uuid, 0,
				serial, "", "", "",
				"", "",
				formattedTime, point_mac)

			sqlAddSensor, err := db.Query(query)
			if err != nil {
				log.Println(err)
				err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			}
			defer sqlAddSensor.Close()

		}); token.Wait() && token.Error() != nil {
			print(token.Error())
		}

		topic = topic + "/data/#"

		token := client.Subscribe(topic, 2, func(client mqtt.Client, msg mqtt.Message) {
			basePath := "storage_data/"
			processMqttMessage(msg, basePath)
		})
		if token.Wait() && token.Error() != nil {
			log.Println(token.Error())
		}

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

func saveImagesPeriodically(client mqtt.Client, topics []string, basePath string) {
	ticker := time.NewTicker(10 * time.Second) // 10초마다 실행
	for {
		select {
		case <-ticker.C:
			for _, topic := range topics {
				token := client.Subscribe(topic, 2, func(client mqtt.Client, msg mqtt.Message) {
					processMqttMessage(msg, basePath)
				})
				if token.Wait() && token.Error() != nil {
					log.Println("Error subscribing to topic:", token.Error())
				}
			}
		}
	}
}

func processMqttMessage(msg mqtt.Message, basePath string) {

	parts := strings.Split(msg.Topic(), "/")
	if len(parts) >= 3 {
		serial := parts[3]

		folderPath := filepath.Join(basePath, serial)
		err := createFolder(folderPath)
		if err != nil {
			log.Println("Error creating folder:", err)
			return
		}

		formattedTime := time.Now().Format("2006-01-02 15:04:05")
		fileName := formattedTime + ".jpg"
		filePath := filepath.Join(folderPath, fileName)

		err = saveImageToFile(filePath, msg.Payload())
		if err != nil {
			log.Println("Error saving image:", err)
		} else {
		}
	}
}

func saveImageToFile(filePath string, data []byte) error {
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
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
	ticker := time.NewTicker(24 * time.Hour) // 24시간마다 실행
	// ticker := time.NewTicker(3 * time.Second) // 3초마다 실행

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
	log.Println(basePath)
	var folders []string
	err := filepath.Walk(basePath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() && path != basePath {
			folders = append(folders, filepath.Base(path))
		}
		return nil
	})
	return folders, err
}

func deleteOldImagesInFolder(folderPath string) {
	err := filepath.Walk(folderPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			if info.ModTime().Before(time.Now().Add(-7 * 24 * time.Hour)) {
				err := os.Remove(path)
				if err != nil {
					fmt.Println("Error deleting file:", err)
				} else {
				}
			}
		}
		return nil
	})
	if err != nil {
		log.Println("Error deleting old images in folder:", err)
	}
}

func SendMessageHandler(title string, body string, style string, topic string) {

	firebaseutil.SendMessage(title, body, style, topic)

}
