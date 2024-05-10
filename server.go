package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"main/utils"

	"main/firebaseutil"

	pb "github.com/ParkByeongKeun/trusafer-idl/maincontrol"
	"github.com/ParkByeongKeun/trusafer-idl/maincontrol/service"
	"github.com/dgrijalva/jwt-go"
	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"gopkg.in/natefinch/lumberjack.v2"

	_ "github.com/go-sql-driver/mysql"
)

func init() {
	mEventEndTimes = make(map[string]int64)
	mStatus = make(map[string]string)
	mImageStatus = make(map[string]string)
	mGroupUUIDs = make(map[string][]string)
	queue = make(chan *write.Point, queueSize)
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

var tokenList []string

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

func main() {
	conf_file := flag.String("config", "/trusafer/config.json", "config file path")
	flag.Parse()
	err := LoadConfiguration(*conf_file)
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(time.Now().UnixNano())

	// Init firebase
	serviceAccountKeyPath := "/trusafer/serviceAccountKey.json"
	err = firebaseutil.InitFirebase(serviceAccountKeyPath)
	if err != nil {
		log.Fatalln("[InitFirebase] initializing app error :", err)
	}

	// Init logfile
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
	logger.Title.Printf("Start Trusafer Server")

	// Connect to influxdb
	_influxdb_client = influxdb2.NewClient(Conf.InfluxDB.Address, Conf.InfluxDB.Token)
	defer _influxdb_client.Close()
	_writeAPI = _influxdb_client.WriteAPIBlocking(Conf.InfluxDB.Org, Conf.InfluxDB.Bucket)
	_queryAPI = _influxdb_client.QueryAPI(Conf.InfluxDB.Org)

	go writeQueue()

	_, err = _influxdb_client.Health(context.Background())
	if err != nil {
		logger.Error.Printf("Failed to connect to InfluxDB: %s", err)
		log.Fatalf("Failed to connect to InfluxDB: %s", err)
	}
	logger.Error.Printf("Connected to InfluxDB")

	// Get AES scecret key
	hash := sha256.New()
	_, err = hash.Write([]byte(Conf.Encryption.Passphrase))
	if err != nil {
		log.Println(err.Error())
	}
	aesSecretKey = hash.Sum(nil)

	// Init MQTT Broker
	clientID := RandomString(15)
	base_topic = "trusafer"
	opts_broker := mqtt.NewClientOptions().AddBroker(Conf.Broker.Address)
	tlsconfig := utils.NewTLSConfig()
	opts_broker.SetClientID(clientID).SetTLSConfig(tlsconfig)
	opts_broker.SetUsername(Conf.Broker.UserId)
	opts_broker.SetPassword(Conf.Broker.UserPassword)
	opts_broker.SetCleanSession(false)

	opts_broker.SetConnectionLostHandler(func(c mqtt.Client, err error) {
		println("mqtt connection lost error: " + err.Error())
	})
	opts_broker.SetReconnectingHandler(func(c mqtt.Client, options *mqtt.ClientOptions) {
		println("mqtt reconnecting...")
	})
	opts_broker.SetOnConnectHandler(func(c mqtt.Client) {
		println("mqtt connected!")

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

	go func() {
		for msg := range messageCh {
			handleMessage(client, msg)
		}
	}()

	client = mqtt.NewClient(opts_broker)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Fatal(token.Error())
	}

	// Init mariadb
	dataSource := fmt.Sprintf("%s:%s@tcp(%s)/%s",
															Conf.Database.Id,
															Conf.Database.Password,
															Conf.Database.Address,
															Conf.Database.Name)
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
	log.Printf("Connected to DB [%s/%s] successfully\n", Conf.Database.Address, Conf.Database.Name)
	createDBTablesIfNotExist()

	// Init gRPC
	creds, sslErr := credentials.NewServerTLSFromFile(Conf.Grpc.CertFile, Conf.Grpc.KeyFile)
	if sslErr != nil {
		log.Fatalf("Failed loading certificates: %v", sslErr)
		return
	}
	opts_grpc := []grpc.ServerOption{}
	jwtManagerAT := service.NewJWTManager(Conf.Jwt.SecretKeyAT, Conf.Jwt.TokenDurationAT)
	interceptorAT := service.NewAuthInterceptor(jwtManagerAT, accessibleRolesForRT())
	opts_grpc = append(opts_grpc, grpc.Creds(creds))
	opts_grpc = append(opts_grpc, grpc.UnaryInterceptor(interceptorAT.Unary()))
	grpcServer := grpc.NewServer()
	lis, err := net.Listen("tcp", ":"+strconv.Itoa(Conf.Grpc.Port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	pb.RegisterMainControlServer(grpcServer, &server{})
	go func() {
		log.Printf("Serving gRPC server on %d port", Conf.Grpc.Port)
		if err := grpcServer.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %s", err)
		}
	}()
	conn, err := grpc.DialContext(
		context.Background(),
		"0.0.0.0:"+strconv.Itoa(Conf.Grpc.Port),
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
	gwPortString := fmt.Sprintf(":%d", Conf.Gw.Port)
	gwServer := &http.Server{
		Addr:    gwPortString,
		Handler: cors(gwmux),
	}

	go func() {
		log.Printf("Serving gRPC-Gateway on " + gwPortString + " port")
		log.Fatalln(gwServer.ListenAndServeTLS(Conf.Grpc.CertFile, Conf.Grpc.KeyFile))
	}()

	// Init Image REST API Server
	http.HandleFunc("/api/main/image/v1/upload_company_image", uploadHandler)
	http.HandleFunc("/api/main/image/v1/get_company_image", imageHandler)
	imPortString := fmt.Sprintf(":%d", Conf.Im.Port)
	httpServer := &http.Server{
		Addr:    imPortString,
		Handler: nil, // Use the default ServeMux
	}
	log.Printf("Serving Image REST API on " + imPortString + " port")
	log.Fatalln(httpServer.ListenAndServeTLS(Conf.Grpc.CertFile, Conf.Grpc.KeyFile))
}
