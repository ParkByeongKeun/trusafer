package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/protobuf/proto"
	"gopkg.in/gomail.v2"

	pb "github.com/ParkByeongKeun/trusafer-idl/maincontrol"
	"github.com/ParkByeongKeun/trusafer-idl/maincontrol/server/service"
	"github.com/dgrijalva/jwt-go"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"

	"github.com/spf13/viper"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var saveImageDir string

type Claims struct {
	Email string `json:"email"`
	jwt.StandardClaims
}

func accessibleRolesForAT() map[string][]string {
	return map[string][]string{
		"admin": {"read", "write", "delete"},
		"user":  {"read"},
	}
}

func uploadFile(w http.ResponseWriter, r *http.Request) {
	// 파일 읽기
	file, handler, err := r.FormFile("file")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// 실제 파일 저장
	f, err := os.OpenFile(handler.Filename, os.O_WRONLY|os.O_CREATE, 0666)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer f.Close()

	// 파일 복사
	io.Copy(f, file)

	// 업로드 완료 메시지 출력
	fmt.Fprintf(w, "File uploaded successfully")
}

func accessibleRolesForRT() map[string][]string {
	return map[string][]string{
		"admin": {"refresh"},
		"user":  {"refresh"},
	}
}

func main() {
	conf_file := flag.String("config", "server/config.json", "config file path")
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

func handleUpload(w http.ResponseWriter, r *http.Request) {
	// Parse the Multipart/form-data request
	err := r.ParseMultipartForm(10 << 20) // 10 MB limit for the entire request
	if err != nil {
		http.Error(w, "Error parsing form", http.StatusBadRequest)
		return
	}

	// Extract Protobuf message from the form field
	protobufData, _, err := r.FormFile("protobuf_data")
	if err != nil {
		http.Error(w, "Error retrieving protobuf data", http.StatusBadRequest)
		return
	}
	defer protobufData.Close()

	protobufBytes, err := ioutil.ReadAll(protobufData)
	if err != nil {
		http.Error(w, "Error reading protobuf data", http.StatusInternalServerError)
		return
	}

	registerer := &pb.Registerer{}
	err = proto.Unmarshal(protobufBytes, registerer)
	if err != nil {
		http.Error(w, "Error unmarshalling protobuf data", http.StatusInternalServerError)
		return
	}

	// Extract the file from the form field
	file, fileHeader, err := r.FormFile("company_number_file")
	if err != nil {
		http.Error(w, "Error retrieving file", http.StatusBadRequest)
		return
	}
	defer file.Close()
	originalFileName := fileHeader.Filename

	fileBytes, err := ioutil.ReadAll(file)
	savePath := filepath.Join("/Users", "bkpark", "trusafer")

	err1 := os.MkdirAll(savePath, 0755)
	if err1 != nil {
		log.Printf("Error creating directory: %v", err)
	}

	filePath := filepath.Join(savePath, originalFileName)

	err = ioutil.WriteFile(filePath, fileBytes, 0644)
	if err != nil {
		log.Printf("Error saving file: %v", err)
	}

	err = sendEmail(filePath)
	if err != nil {
		log.Printf("Error sending email: %v", err)
	}

	// Process the Protobuf message and file as needed
	fmt.Printf("Received Registerer:\n%+v\n", registerer)
	// fileBytes now contains the binary data of the file

	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Upload successful"))
}

func sendEmail(attachmentPath string) error {
	from := "ijoon.helper@gmail.com"
	to := []string{"yot132@ijoon.net"}
	subject := "File Attachment"
	body := "Please find the attached file."

	mailer := gomail.NewMessage()
	mailer.SetHeader("From", from)
	mailer.SetHeader("To", to...)
	mailer.SetHeader("Subject", subject)
	mailer.SetBody("text/plain", body)

	attachmentName := filepath.Base(attachmentPath)
	mailer.Attach(attachmentPath, gomail.Rename(attachmentName))

	smtpHost := "smtp.gmail.com"
	smtpPort := 587
	smtpUsername := "ijoon.helper@gmail.com"
	smtpPassword := "ycbygboiryotnjyn"

	dialer := gomail.NewDialer(smtpHost, smtpPort, smtpUsername, smtpPassword)

	dialer.TLSConfig = nil

	if err := dialer.DialAndSend(mailer); err != nil {
		return err
	}

	return nil
}
