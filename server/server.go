package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	pb "github.com/ParkByeongKeun/trusafer-idl"
	"github.com/ParkByeongKeun/trusafer-idl/maincontrol/server/service"

	_ "github.com/go-sql-driver/mysql"
)

var db *sql.DB
var saveImageDir string

const (
	secretKey     = "secret"
	tokenDuration = (24 * time.Hour) * 365
)

// func seedUsers(userStore service.UserStore) error {
// 	err := createUser(userStore, "admin1", "secret", "admin")
// 	createUser(userStore, "admin1", "secret", "admin")
// 	if err != nil {
// 		return err
// 	}
// 	return createUser(userStore, "ksf", "1234", "user")
// }

// func createUser(userStore service.UserStore, username, password, role string) error {

// 	user, err := service.NewUser(username, password, role)
// 	if err != nil {
// 		return err
// 	}
// 	return userStore.Save(user)
// }

func accessibleRoles() map[string][]string {
	return map[string][]string{
		"item1": {"admin"},
		"item2": {"admin"},
		"item3": {"admin", "user"},
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

	// userStore := service.NewInMemoryUserStore()
	// err = seedUsers(userStore)
	// if err != nil {
	// 	log.Fatal("cannot seed users")
	// }
	// DB open pool 만들기
	dataSource := fmt.Sprintf("%s:%s@tcp(%s)/%s", dbID, dbPW, dbAddr, dbName)
	db, err = sql.Open("mysql", dataSource)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// ping 날려서 DB 연결 확인
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = db.PingContext(ctx)
	if err != nil {
		log.Fatalf("DB ping errors: %s", err)
		return
	}
	log.Printf("Connected to DB [%s/%s] successfully\n", dbAddr, dbName)

	// create table (barn, history, registerer)
	createDBTablesIfNotExist()

	// tls := false
	// if tls {
	certFile := grpcCert
	keyFile := grpcKey
	creds, sslErr := credentials.NewServerTLSFromFile(certFile, keyFile)
	if sslErr != nil {
		log.Fatalf("Failed loading certificates: %v", sslErr)
		return
	}
	// }
	// var opts []grpc.ServerOption

	opts := []grpc.ServerOption{}
	jwtManager := service.NewJWTManager(secretKey, tokenDuration)
	// authServer := service.NewAuthServer(userStore, jwtManager)
	interceptor := service.NewAuthInterceptor(jwtManager, accessibleRoles())
	opts = append(opts, grpc.Creds(creds), grpc.UnaryInterceptor(interceptor.Unary()))
	grpcServer := grpc.NewServer(opts...)
	// gRPC 서버 네트워크 연결 대기
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(grpcPort))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	// auth_pb.RegisterAuthServiceServer(grpcServer, authServer)
	pb.RegisterMainControlServer(grpcServer, &server{})

	log.Printf("start gRPC server on %d port", grpcPort)
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %s", err)
	}
}
