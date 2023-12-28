package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"time"

	"main/firebaseutil"

	pb "github.com/ParkByeongKeun/trusafer-idl/maincontrol"
	"github.com/dgrijalva/jwt-go"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type MainListResponseMapping struct {
	mu      sync.Mutex
	mapping map[string]*pb.MainListResponse
}

// ============================MainList Mapping====================================================
func NewMainListResponseMapping() *MainListResponseMapping {
	return &MainListResponseMapping{
		mapping: make(map[string]*pb.MainListResponse),
	}
}

func (m *MainListResponseMapping) AddMapping(registererUUID string, response *pb.MainListResponse) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mapping[registererUUID] = response
}

func (m *MainListResponseMapping) GetMapping(registererUUID string) (*pb.MainListResponse, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	response, ok := m.mapping[registererUUID]
	return response, ok
}

func (m *MainListResponseMapping) RemoveMapping(registererUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mapping, registererUUID)
}

func DeleteMainList(s *server, ctx context.Context) {
	registererinfo, err := s.ReadRegisterer(ctx, &pb.ReadRegistererRequest{
		Name: "check",
	})
	if err != nil {
		log.Println(err)
	}

	defer mainListMapping.RemoveMapping(registererinfo.GetRegistererInfo().GetUuid())
}

var mainListMapping = NewMainListResponseMapping()

// ============================MainList Mapping====================================================

type server struct {
	pb.UnimplementedMainControlServer
}

func boolToInt(value bool) int64 {
	if value {
		return 1
	}
	return 0
}

func intToBool(value uint64) bool {
	return value != 0
}

func (s *server) getPermission(ctx context.Context) pb.Permission {
	md, ok := metadata.FromIncomingContext(ctx)
	if md["authorization"] == nil {
		return pb.Permission{}
	}
	authHeaders, ok := md["authorization"]
	if !ok || len(authHeaders) == 0 {
	}
	token := authHeaders[0]
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte("secret2"), nil
	})
	if err != nil {
	}
	registererinfo, err := s.ReadRegisterer(ctx, &pb.ReadRegistererRequest{
		Name: "check",
	})
	if err != nil {
		log.Println(err)
	}
	var permission pb.Permission
	permission.User = registererinfo.GetRegistererInfo().GetPUser()
	permission.Permission = registererinfo.GetRegistererInfo().GetPPermission()
	permission.SensorCreate = registererinfo.GetRegistererInfo().GetPSensorCreate()
	permission.SensorInfo = registererinfo.GetRegistererInfo().GetPSensorInfo()
	permission.IpModule = registererinfo.GetRegistererInfo().GetPIpModule()
	permission.Threshold = registererinfo.GetRegistererInfo().GetPThreshold()
	permission.SensorHistory = registererinfo.GetRegistererInfo().GetPSensorHistory()
	return permission
}

func (s *server) CreateRegisterer(ctx context.Context, in *pb.CreateRegistererRequest) (*pb.CreateRegistererResponse, error) {
	log.Printf("Received AddRegisterer: %s, %s, %s, %s, %s, %d, %s",
		in.Registerer.GetUuid(), in.Registerer.GetAuthEmail(), in.Registerer.GetCompanyName(),
		in.Registerer.GetCompanyNumber(), in.Registerer.GetStatus(), boolToInt(in.Registerer.GetIsAlarm()),
		in.Registerer.GetPermissionUuid())
	var uuid = uuid.New()
	query := fmt.Sprintf(`
		INSERT INTO registerer SET 
			uuid = '%s',
			auth_email = '%s',
			company_name = '%s',
			company_number = '%s',
			status = %d,
			is_alarm = '%d',
			permission_uuid = '%s',
			name = '%s'
		`,
		uuid.String(), in.Registerer.GetAuthEmail(), in.Registerer.GetCompanyName(),
		in.Registerer.GetCompanyNumber(), in.Registerer.GetStatus(), boolToInt(in.Registerer.GetIsAlarm()),
		in.Registerer.GetPermissionUuid(), in.Registerer.GetName())
	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Not Found Data: %v", err)
		return nil, err
	}
	defer sqlAddRegisterer.Close()
	return &pb.CreateRegistererResponse{}, nil
}
func (s *server) UpdateRegisterer(ctx context.Context, in *pb.UpdateRegistererRequest) (*pb.UpdateRegistererResponse, error) {
	var permission_uuid string
	permission := s.getPermission(ctx)
	registererinfo, err := s.ReadRegisterer(ctx, &pb.ReadRegistererRequest{
		Name: "check",
	})
	if registererinfo.GetRegistererInfo().GetUuid() != in.Registerer.GetUuid() {
		if !permission.User {
			log.Println("err permission")
			return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
		}
		if !permission.Permission {
			query := fmt.Sprintf(`
        SELECT permission_uuid
        FROM registerer
        WHERE uuid = '%s'
        LIMIT 1
    `, in.Registerer.GetUuid())
			rows, err := db.Query(query)
			if err != nil {
				log.Println(err)
				return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			}
			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(&permission_uuid)
				if err != nil {
					log.Println(err)
					return nil, err
				}
			}
			if permission_uuid != in.Registerer.GetPermissionUuid() {
				log.Println("err permission")
				return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
			}
		}
	}
	query := fmt.Sprintf(`
		UPDATE registerer SET
			auth_email = '%s',
			company_name = '%s',
			company_number = '%s',
			status = %d,
			is_alarm = '%d',
			permission_uuid = '%s',
			name = '%s'
		WHERE uuid = '%s'
		`,
		in.Registerer.GetAuthEmail(), in.Registerer.GetCompanyName(),
		in.Registerer.GetCompanyNumber(), in.Registerer.GetStatus(), boolToInt(in.Registerer.GetIsAlarm()),
		in.Registerer.GetPermissionUuid(), in.Registerer.GetName(), in.Registerer.GetUuid())

	sqlUpdateRegisterer, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	affectedCount, err := sqlUpdateRegisterer.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	log.Println("update users complete: ", affectedCount)
	return &pb.UpdateRegistererResponse{}, nil
}

func (s *server) DeleteRegisterer(ctx context.Context, in *pb.DeleteRegistererRequest) (*pb.DeleteRegistererResponse, error) {
	DeleteMainList(s, ctx)
	query := fmt.Sprintf(`
		DELETE FROM registerer
		WHERE uuid = '%s'
		`,
		in.GetRegistererUuid())
	sqlDeleteRegisterer, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeleteRegisterer.RowsAffected()
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	fmt.Println("delete count: ", nRow)
	return &pb.DeleteRegistererResponse{}, nil
}

func (s *server) ReadRegisterer(ctx context.Context, in *pb.ReadRegistererRequest) (*pb.ReadRegistererResponse, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Errorf(codes.Unauthenticated, "not read metadata")
	}
	if md["authorization"] == nil {
		return nil, status.Errorf(codes.Unauthenticated, "not read metadata")
	}
	authHeaders, ok := md["authorization"]
	if !ok || len(authHeaders) == 0 {
		return nil, status.Errorf(codes.Unauthenticated, "Authentication token not provided")
	}
	token := authHeaders[0]
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte("secret2"), nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
	}
	log.Printf("Received GetRegisterer")
	response := &pb.ReadRegistererResponse{}
	var uuid_ string
	var auth_email string
	var company_name string
	var company_number string
	var status_ pb.RegistererStatus
	var is_alarm uint64
	var permission_uuid string
	var name string

	var user uint64
	var permission uint64
	var sensor_create uint64
	var sensor_info uint64
	var ip_module uint64
	var threshold uint64
	var sensor_history uint64

	query := fmt.Sprintf(`
		SELECT uuid, auth_email, company_name, company_number, status, is_alarm, permission_uuid, name  
		FROM registerer 
		WHERE auth_email = '%s'
		`,
		claims.Email)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, status.Errorf(codes.Internal, "Failed to fetch registerer information: %v", err)
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&uuid_, &auth_email, &company_name, &company_number, &status_, &is_alarm, &permission_uuid, &name)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan registerer rows: %v", err)
		}
		if permission_uuid != "" {
			permissionQuery := fmt.Sprintf(`
				SELECT user, permission, sensor_create, sensor_info, ip_module, threshold, sensor_history  
				FROM permission 
				WHERE uuid = '%s'
				`, permission_uuid)
			permissionRows, err := db.Query(permissionQuery)
			if err != nil {
				log.Println(err)
				return nil, status.Errorf(codes.Internal, "Failed to fetch permission information: %v", err)
			}
			defer permissionRows.Close()
			for permissionRows.Next() {
				err := permissionRows.Scan(&user, &permission, &sensor_create, &sensor_info, &ip_module, &threshold, &sensor_history)
				if err != nil {
					log.Println(err)
					return nil, status.Errorf(codes.Internal, "Failed to scan permission rows: %v", err)
				}
			}
		}
		registerer := &pb.RegistererInfo{
			Uuid:           uuid_,
			AuthEmail:      auth_email,
			CompanyName:    company_name,
			CompanyNumber:  company_number,
			Status:         status_,
			IsAlarm:        intToBool(is_alarm),
			Name:           name,
			PUser:          intToBool(user),
			PPermission:    intToBool(permission),
			PSensorCreate:  intToBool(sensor_create),
			PSensorInfo:    intToBool(sensor_info),
			PIpModule:      intToBool(ip_module),
			PThreshold:     intToBool(threshold),
			PSensorHistory: intToBool(sensor_history),
		}
		response.RegistererInfo = registerer
	}

	if response.RegistererInfo == nil {
		defaultPermissionQuery := fmt.Sprintf(`
			SELECT uuid 
			FROM permission 
			WHERE name = 'default'
			`)

		rows, err := db.Query(defaultPermissionQuery)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to fetch default permission UUID: %v", err)
		}
		defer rows.Close()
		var firstPermissionUUID string
		for rows.Next() {
			err := rows.Scan(&firstPermissionUUID)
			if err != nil {
				log.Println(err)
				return nil, status.Errorf(codes.Internal, "Failed to scan default permission rows: %v", err)
			}
		}
		newUUID := uuid.New()
		genUUID := newUUID.String()
		query := fmt.Sprintf(`
			INSERT INTO registerer SET 
				uuid = '%s',
				auth_email = '%s',
				company_name = '%s',
				company_number = '%s',
				status = %d,
				is_alarm = '%d',
				permission_uuid = '%s',
				name = '%s'
			`,
			genUUID, claims.Email, "", "", 2, 0, firstPermissionUUID, in.GetName())

		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to insert new registerer: %v", err)
		}
		defer sqlAddRegisterer.Close()
		registerer := &pb.RegistererInfo{
			Uuid:          genUUID,
			AuthEmail:     claims.Email,
			CompanyName:   "",
			CompanyNumber: "",
			Status:        2,
			IsAlarm:       intToBool(0),
			Name:          in.GetName(),
		}
		response.RegistererInfo = registerer
	}
	response.PermissionUuid = permission_uuid
	return response, nil
}

func (s *server) ReadRegistererList(ctx context.Context, in *pb.ReadRegistererListRequest) (*pb.ReadRegistererListResponse, error) {
	log.Printf("Received GetRegistererList: success")
	response := &pb.ReadRegistererListResponse{}
	var uuid string
	var authEmail string
	var companyName string
	var companyNumber string
	var status_ pb.RegistererStatus
	var isAlarm uint64
	var permissionUUID string
	var name string
	query := `
		SELECT uuid, auth_email, company_name, company_number, status, is_alarm, permission_uuid, name  
		FROM registerer 
	`
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&uuid, &authEmail, &companyName, &companyNumber, &status_, &isAlarm, &permissionUUID, &name)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}
		registerer := &pb.Registerer{
			Uuid:           uuid,
			AuthEmail:      authEmail,
			CompanyName:    companyName,
			CompanyNumber:  companyNumber,
			Status:         status_,
			IsAlarm:        intToBool(isAlarm),
			PermissionUuid: permissionUUID,
			Name:           name,
		}
		response.RegistererList = append(response.RegistererList, registerer)
	}
	return response, nil
}

func (s *server) CreatePlace(ctx context.Context, in *pb.CreatePlaceRequest) (*pb.CreatePlaceResponse, error) {
	DeleteMainList(s, ctx)
	log.Printf("Received AddPlace: %s, %s, %s, %s, %s",
		in.Place.GetUuid(), in.Place.GetName(), in.Place.GetAddress(),
		in.Place.GetRegistererUuid(), in.Place.GetRegisteredTime())
	var uuid = uuid.New()
	response := &pb.CreatePlaceResponse{}
	query := fmt.Sprintf(`
		INSERT INTO place SET
			uuid = '%s', 
			name = '%s',
			address = '%s',
			registerer_uuid = '%s',
			registered_time = '%s'
		`,
		uuid.String(), in.Place.GetName(), in.Place.GetAddress(),
		in.Place.GetRegistererUuid(), in.Place.GetRegisteredTime())
	sqlAddPlace, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	response.Uuid = uuid.String()
	defer sqlAddPlace.Close()
	return response, nil
}

func (s *server) UpdatePlace(ctx context.Context, in *pb.UpdatePlaceRequest) (*pb.UpdatePlaceResponse, error) {
	DeleteMainList(s, ctx)
	log.Printf("Received UpdatePlace: %s", in.Place.GetUuid())
	query := fmt.Sprintf(`
		UPDATE place SET
			name = '%s',
			address = '%s',
			registerer_uuid = '%s',
			registered_time = '%s'
		WHERE uuid = '%s'
		`,
		in.Place.GetName(), in.Place.GetAddress(),
		in.Place.GetRegistererUuid(), in.Place.GetRegisteredTime(),
		in.Place.GetUuid())
	sqlUpdatePlace, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	affectedCount, err := sqlUpdatePlace.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	log.Println("update place complete: ", affectedCount)
	return &pb.UpdatePlaceResponse{}, nil
}

func (s *server) DeletePlace(ctx context.Context, in *pb.DeletePlaceRequest) (*pb.DeletePlaceResponse, error) {
	DeleteMainList(s, ctx)
	log.Printf("Received DeletePlace: %s", in.GetPlaceUuid())
	query := fmt.Sprintf(`
		DELETE FROM place
		WHERE uuid = '%s'
		`,
		in.GetPlaceUuid())
	sqlDeletePlace, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeletePlace.RowsAffected()
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	fmt.Println("delete count : ", nRow)
	return &pb.DeletePlaceResponse{}, nil
}

func (s *server) ReadPlace(ctx context.Context, in *pb.ReadPlaceRequest) (*pb.ReadPlaceResponse, error) {
	log.Printf("Received GetPlace: %s", in.GetPlaceUuid())
	response := &pb.ReadPlaceResponse{}
	var uuid string
	var name string
	var address string
	var registerer_uuid string
	var registered_time string
	query := fmt.Sprintf(`
		SELECT uuid, name, address, registerer_uuid, registered_time 
		FROM place 
		WHERE uuid = '%s'
		`,
		in.GetPlaceUuid())
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&uuid, &name, &address, &registerer_uuid, &registered_time)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}
		place := &pb.Place{}
		place.Uuid = uuid
		place.Name = name
		place.Address = address
		place.RegistererUuid = registerer_uuid
		place.RegisteredTime = registered_time
		response.Place = place
	}
	return response, nil
}

func (s *server) ReadPlaceList(ctx context.Context, in *pb.ReadPlaceListRequest) (*pb.ReadPlaceListResponse, error) {
	log.Printf("Received GetPlaceList")
	response := &pb.ReadPlaceListResponse{}
	var uuid string
	var name string
	var address string
	var registererUUID string
	var registeredTime string
	query := fmt.Sprintf(`
		SELECT uuid, name, address, registerer_uuid, registered_time 
		FROM place 
		`)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&uuid, &name, &address, &registererUUID, &registeredTime)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}
		placeList := &pb.Place{
			Uuid:           uuid,
			Name:           name,
			Address:        address,
			RegistererUuid: registererUUID,
			RegisteredTime: registeredTime,
		}
		response.PlaceList = append(response.PlaceList, placeList)
	}
	return response, nil
}

func (s *server) CreateSettop(ctx context.Context, in *pb.CreateSettopRequest) (*pb.CreateSettopResponse, error) {
	DeleteMainList(s, ctx)
	log.Printf("Received AddSettop: %s, %s, %s, %s, %s, %s",
		in.Settop.GetUuid(), in.Settop.GetPlaceUuid(), in.Settop.GetSerial(),
		in.Settop.GetRoom(), in.Settop.GetFloor(), in.Settop.GetRegisteredTime())
	var uuid = uuid.New()
	response := &pb.CreateSettopResponse{}
	query := fmt.Sprintf(`
		INSERT INTO settop SET
			uuid = '%s', 
			place_uuid = '%s',
			serial = '%s',
			room = '%s',
			floor = '%s',
			mac1 = '%s',
			mac2 = '%s',
			is_alive = '%d',
			latest_version = '%s',
			registered_time = '%s'
		`,
		uuid.String(), in.Settop.GetPlaceUuid(), in.Settop.GetSerial(),
		in.Settop.GetRoom(), in.Settop.GetFloor(), in.Settop.GetMac1(),
		in.Settop.GetMac2(), boolToInt(in.Settop.GetIsAlive()), in.Settop.GetLatestVersion(), in.Settop.GetRegisteredTime())
	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	response.Uuid = uuid.String()
	defer sqlAddRegisterer.Close()
	return response, nil
}

func (s *server) UpdateSettop(ctx context.Context, in *pb.UpdateSettopRequest) (*pb.UpdateSettopResponse, error) {
	DeleteMainList(s, ctx)

	permission := s.getPermission(ctx)
	var latestVersion string
	if !permission.IpModule {
		query := fmt.Sprintf(`
        SELECT latest_version
        FROM settop
        WHERE Uuid = '%s'
        LIMIT 1
    `, in.Settop.GetUuid())
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&latestVersion)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}
		if latestVersion != in.Settop.LatestVersion {
			log.Println("err permission")
			return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
		}
	}
	log.Printf("Received UpdateSettop: %s", in.Settop.GetUuid())
	query := fmt.Sprintf(`
		UPDATE settop SET
			place_uuid = '%s', 
			serial = '%s',
			room = '%s',
			floor = '%s',
			mac1 = '%s',
			mac2 = '%s',
			is_alive = '%d',
			latest_version = '%s',
			registered_time = '%s'
		WHERE uuid = '%s'
		`,
		in.Settop.GetPlaceUuid(), in.Settop.GetSerial(),
		in.Settop.GetRoom(), in.Settop.GetFloor(), in.Settop.GetMac1(), in.Settop.GetMac2(),
		boolToInt(in.Settop.GetIsAlive()), in.Settop.GetLatestVersion(), in.Settop.GetRegisteredTime(), in.Settop.GetUuid())
	sqlUpdateSettop, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	affectedCount, err := sqlUpdateSettop.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	log.Println("update settop complete: ", affectedCount)
	return &pb.UpdateSettopResponse{}, nil
}

func (s *server) DeleteSettop(ctx context.Context, in *pb.DeleteSettopRequest) (*pb.DeleteSettopResponse, error) {
	DeleteMainList(s, ctx)
	log.Printf("Received DeleteSettop: %s", in.GetSettopUuid())
	query := fmt.Sprintf(`
		DELETE FROM settop
		WHERE uuid = '%s'
		`,
		in.GetSettopUuid())

	sqlDeleteSettop, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeleteSettop.RowsAffected()
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	fmt.Println("delete count : ", nRow)
	return &pb.DeleteSettopResponse{}, nil
}

func (s *server) ReadSettop(ctx context.Context, in *pb.ReadSettopRequest) (*pb.ReadSettopResponse, error) {
	log.Printf("Received GetSettop: %s", in.GetSettopUuid())
	response := &pb.ReadSettopResponse{}
	var uuid string
	var place_uuid string
	var serial string
	var room string
	var floor string
	var mac1 string
	var mac2 string
	var is_alive bool
	var latest_version string
	var registered_time string
	query := fmt.Sprintf(`
		SELECT uuid, place_uuid, serial, room, floor, mac1, mac2, is_alive, latest_version, registered_time  
		FROM settop 
		WHERE uuid = '%s'
		`,
		in.GetSettopUuid())
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&uuid, &place_uuid, &serial, &room, &floor, &mac1, &mac2, &is_alive, &latest_version, &registered_time)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}
		var place_name string
		var place_address string
		query1 := fmt.Sprintf(`
		SELECT name, address
		FROM place 
		WHERE uuid = '%s'
	`,
			place_uuid)
		rows1, err := db.Query(query1)

		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer rows1.Close()
		for rows1.Next() {
			err := rows1.Scan(&place_name, &place_address)
			if err != nil {
				log.Println(err)
				err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
				return nil, err
			}
		}
		settop := &pb.SettopInfo{}
		settop.Uuid = uuid
		settop.PlaceUuid = place_uuid
		settop.Serial = serial
		settop.Room = room
		settop.Floor = floor
		settop.Mac1 = mac1
		settop.Mac2 = mac2
		settop.IsAlive = is_alive
		settop.LatestVersion = latest_version
		settop.RegisteredTime = registered_time
		settop.PlaceAddress = place_address
		settop.PlaceName = place_name
		response.Settop = settop
	}
	return response, nil
}

func (s *server) ReadSettopList(ctx context.Context, in *pb.ReadSettopListRequest) (*pb.ReadSettopListResponse, error) {
	log.Printf("Received GetSettopList: success")
	response := &pb.ReadSettopListResponse{}

	var uuid string
	var place_uuid string
	var serial string
	var room string
	var floor string
	var mac1 string
	var mac2 string
	var is_alive bool
	var latest_version string
	var registered_time string

	query := fmt.Sprintf(`
		SELECT uuid, place_uuid, serial, room, floor, mac1, mac2, is_alive, latest_version, registered_time  
		FROM settop 
		`)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &place_uuid, &serial, &room, &floor, &mac1, &mac2, &is_alive, &latest_version, &registered_time)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}
		var place_name string
		var place_address string
		query1 := fmt.Sprintf(`
			SELECT name, address
			FROM place 
			WHERE uuid = '%s'
		`,
			place_uuid)

		rows1, err := db.Query(query1)

		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer rows1.Close()

		for rows1.Next() {
			err := rows1.Scan(&place_name, &place_address)
			if err != nil {
				log.Println(err)
				err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
				return nil, err
			}
		}

		settopList := &pb.SettopInfo{}
		settopList.Uuid = uuid
		settopList.PlaceUuid = place_uuid
		settopList.Serial = serial
		settopList.Room = room
		settopList.Floor = floor
		settopList.Mac1 = mac1
		settopList.Mac2 = mac2
		settopList.IsAlive = is_alive
		settopList.LatestVersion = latest_version
		settopList.RegisteredTime = registered_time
		settopList.PlaceAddress = place_address
		settopList.PlaceName = place_name
		response.SettopList = append(response.SettopList, settopList)
	}

	return response, nil
}

func (s *server) CreateSensor(ctx context.Context, in *pb.CreateSensorRequest) (*pb.CreateSensorResponse, error) {
	DeleteMainList(s, ctx)

	permission := s.getPermission(ctx)
	if !permission.SensorCreate {
		log.Println("err permission")
		return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
	}
	log.Printf("Received AddSensor: %s, %s, %d, %s, %s, %s, %s, %s",
		in.Sensor.GetUuid(), in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(),
		in.Sensor.GetLatestVersion(), in.Sensor.GetRegisteredTime())
	var uuid = uuid.New()
	response := &pb.CreateSensorResponse{}
	var thresholds []*pb.Threshold

	for _, threshold := range in.Sensor.Thresholds {
		thresholds = append(thresholds, threshold)
	}

	if len(thresholds) < 9 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request")
	}
	query := fmt.Sprintf(`
		INSERT INTO sensor SET
			uuid = '%s', 
			settop_uuid = '%s',
			status = '%d',
			serial = '%s',
			ip_address = '%s',
			location = '%s',
			latest_version = '%s',
			registered_time = '%s',
			mac = '%s'
		`,
		uuid.String(), in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(), in.Sensor.GetLatestVersion(),
		in.Sensor.GetRegisteredTime(), in.Sensor.IpModuleMac)

	sqlAddSensor, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer sqlAddSensor.Close()

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
		uuid.String(), thresholds[0].GetTempWarning(), thresholds[0].GetTempDanger(),
		thresholds[1].GetTempWarning(), thresholds[1].GetTempDanger(),
		thresholds[2].GetTempWarning(), thresholds[2].GetTempDanger(),
		thresholds[3].GetTempWarning(), thresholds[3].GetTempDanger(),
		thresholds[4].GetTempWarning(), thresholds[4].GetTempDanger(),
		thresholds[5].GetTempWarning(), thresholds[5].GetTempDanger(),
		thresholds[6].GetTempWarning(), thresholds[6].GetTempDanger(),
		thresholds[7].GetTempWarning(), thresholds[7].GetTempDanger(),
		thresholds[8].GetTempWarning(), thresholds[8].GetTempDanger())

	sqlThresh, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer sqlThresh.Close()

	response.Uuid = uuid.String()

	return response, nil
}

func (s *server) UpdateSensor(ctx context.Context, in *pb.UpdateSensorRequest) (*pb.UpdateSensorResponse, error) {
	DeleteMainList(s, ctx)

	permission := s.getPermission(ctx)
	if !permission.SensorInfo {
		log.Println("err permission")
		return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
	}
	var thresholds []*pb.Threshold

	for _, threshold := range in.Sensor.Thresholds {
		thresholds = append(thresholds, threshold)
	}

	if len(thresholds) < 9 {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request")
	}
	log.Printf("Received UpdateSensor: %s, %s, %d, %s, %s, %s, %s, %s",
		in.Sensor.GetUuid(), in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(),
		in.Sensor.GetLatestVersion(), in.Sensor.GetRegisteredTime())

	if !permission.Threshold {
		sensorResponse, _ := s.ReadSensor(ctx, &pb.ReadSensorRequest{
			SensorUuid: in.GetSensor().GetUuid(),
		})

		if !reflect.DeepEqual(sensorResponse.Sensor.GetThresholds(), in.Sensor.GetThresholds()) {
			log.Println("err permission")
			return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
		}
	}
	log.Printf("Received UpdateSensor: %s", in.Sensor.GetUuid())
	query := fmt.Sprintf(`
		UPDATE sensor SET
			settop_uuid = '%s',
			status = '%d',
			serial = '%s',
			ip_address = '%s',
			location = '%s',
			latest_version = '%s',
			registered_time = '%s',
			mac = '%s'
		WHERE uuid = '%s'
		`,
		in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(), in.Sensor.GetLatestVersion(),
		in.Sensor.GetRegisteredTime(), in.Sensor.IpModuleMac, in.Sensor.GetUuid())

	sqlUpdateSensor, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	affectedCount, err := sqlUpdateSensor.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}

	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM threshold WHERE sensor_uuid = '%s'", in.Sensor.GetUuid())
	var count int
	if err := db.QueryRow(checkQuery).Scan(&count); err != nil {
		log.Println(err)
		return nil, status.Errorf(codes.Internal, "Internal Server Error: %v", err)
	}

	if count == 0 {
		// 레코드가 없으면 INSERT 수행
		insertQuery := fmt.Sprintf(`
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
			in.Sensor.GetUuid(), thresholds[0].GetTempWarning(), thresholds[0].GetTempDanger(),
			thresholds[1].GetTempWarning(), thresholds[1].GetTempDanger(),
			thresholds[2].GetTempWarning(), thresholds[2].GetTempDanger(),
			thresholds[3].GetTempWarning(), thresholds[3].GetTempDanger(),
			thresholds[4].GetTempWarning(), thresholds[4].GetTempDanger(),
			thresholds[5].GetTempWarning(), thresholds[5].GetTempDanger(),
			thresholds[6].GetTempWarning(), thresholds[6].GetTempDanger(),
			thresholds[7].GetTempWarning(), thresholds[7].GetTempDanger(),
			thresholds[8].GetTempWarning(), thresholds[8].GetTempDanger())

		if _, err := db.Exec(insertQuery); err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		}
	} else {
		query = fmt.Sprintf(`
	UPDATE threshold SET
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
			WHERE sensor_uuid = '%s'

		`,
			thresholds[0].GetTempWarning(), thresholds[0].GetTempDanger(),
			thresholds[1].GetTempWarning(), thresholds[1].GetTempDanger(),
			thresholds[2].GetTempWarning(), thresholds[2].GetTempDanger(),
			thresholds[3].GetTempWarning(), thresholds[3].GetTempDanger(),
			thresholds[4].GetTempWarning(), thresholds[4].GetTempDanger(),
			thresholds[5].GetTempWarning(), thresholds[5].GetTempDanger(),
			thresholds[6].GetTempWarning(), thresholds[6].GetTempDanger(),
			thresholds[7].GetTempWarning(), thresholds[7].GetTempDanger(),
			thresholds[8].GetTempWarning(), thresholds[8].GetTempDanger(), in.Sensor.GetUuid())

		sqlThresh, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer sqlThresh.Close()
	}

	var settop_serial string
	query = fmt.Sprintf(`
		SELECT serial
		FROM settop 
		WHERE uuid = '%s'
		`,
		in.Sensor.GetSettopUuid())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&settop_serial)
		if err != nil {
			log.Println(err)
		}
	}
	//============================= mqtt publish //=============================//=============================
	set_topic := "trusafer/" + settop_serial + "/" + in.Sensor.IpModuleMac + "/" + in.Sensor.GetSerial() + "/threshold9/set"
	thresholdsBytes, err := json.Marshal(thresholds)

	var jsonData []map[string]string
	if err := json.Unmarshal(thresholdsBytes, &jsonData); err != nil {
		fmt.Println("JSON Unmarshal error:", err)
	}

	result := map[string][]int{
		"danger":  make([]int, len(jsonData)),
		"warning": make([]int, len(jsonData)),
	}

	for i, item := range jsonData {
		danger, _ := strconv.Atoi(item["temp_danger"])
		warning, _ := strconv.Atoi(item["temp_warning"])

		result["danger"][i] = danger
		result["warning"][i] = warning
	}

	resultJSON, err := json.Marshal(result)
	if err != nil {
		log.Println("JSON Marshal error:", err)
	}

	pub_token := client.Publish(set_topic, 0, false, resultJSON)

	go func() {
		_ = pub_token.Wait() // Can also use '<-t.Done()' in releases > 1.2.0
		if pub_token.Error() != nil {
			log.Println(pub_token.Error()) // Use your preferred logging technique (or just fmt.Printf)
		}
		// time.Sleep(10 * time.Second)

	}()
	//============================= mqtt publish //=============================//=============================
	log.Println("update sensor complete: ", affectedCount)

	return &pb.UpdateSensorResponse{}, nil
}

func (s *server) DeleteSensor(ctx context.Context, in *pb.DeleteSensorRequest) (*pb.DeleteSensorResponse, error) {
	DeleteMainList(s, ctx)

	log.Printf("Received DeleteSensor: %s", in.GetSensorUuid())

	query := fmt.Sprintf(`
		DELETE FROM sensor
		WHERE uuid = '%s'
		`,
		in.GetSensorUuid())

	sqlDeleteSensor, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeleteSensor.RowsAffected()
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}

	query = fmt.Sprintf(`
		DELETE FROM threshold
		WHERE sensor_uuid = '%s'
		`,
		in.GetSensorUuid())

	sqlDeleteThreshold, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err = sqlDeleteThreshold.RowsAffected()
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	fmt.Println("delete count : ", nRow)
	return &pb.DeleteSensorResponse{}, nil
}

func (s *server) ReadSensor(ctx context.Context, in *pb.ReadSensorRequest) (*pb.ReadSensorResponse, error) {
	log.Printf("Received GetSensor: %s", in.GetSensorUuid())
	response := &pb.ReadSensorResponse{}

	var uuid string
	var settop_uuid string
	var status_ uint64
	var serial string
	var ip_address string
	var location string
	var latest_version string
	var registered_time string
	var ip_module_mac string

	var thresholds []*pb.Threshold

	query := fmt.Sprintf(`
		SELECT temp_warning1, temp_danger1, temp_warning2, temp_danger2, temp_warning3,
			   temp_danger3, temp_warning4, temp_danger4, temp_warning5, temp_danger5,
			   temp_warning6, temp_danger6, temp_warning7, temp_danger7, temp_warning8,
			   temp_danger8, temp_warning9, temp_danger9
		FROM threshold 
		WHERE sensor_uuid = '%s'
		`,
		in.GetSensorUuid())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
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
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
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

	}
	query = fmt.Sprintf(`
		SELECT uuid, settop_uuid, status, serial, ip_address, location, latest_version, registered_time,mac 
		FROM sensor 
		WHERE uuid = '%s'
		`,
		in.GetSensorUuid())

	rows, err = db.Query(query)

	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &settop_uuid, &status_, &serial, &ip_address, &location, &latest_version, &registered_time, &ip_module_mac)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}

		sensor := &pb.Sensor{}
		sensor.Uuid = uuid
		sensor.SettopUuid = settop_uuid
		sensor.Status = pb.SensorStatus(status_)
		sensor.Serial = serial
		sensor.IpAddress = ip_address
		sensor.Location = location
		sensor.LatestVersion = latest_version
		sensor.RegisteredTime = registered_time
		sensor.IpModuleMac = ip_module_mac
		sensor.Thresholds = thresholds
		response.Sensor = sensor
	}
	var settop_serial string
	query = fmt.Sprintf(`
		SELECT serial  
		FROM settop 
		WHERE uuid = '%s'
		`, response.Sensor.GetSettopUuid())

	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&settop_serial)
		if err != nil {
			log.Println(err)
		}
	}
	response.SettopSerial = settop_serial

	return response, nil
}

func (s *server) ReadSensorList(ctx context.Context, in *pb.ReadSensorListRequest) (*pb.ReadSensorListResponse, error) {
	log.Printf("Received GetSensorList: success")
	response := &pb.ReadSensorListResponse{}

	var uuid string
	var settop_uuid string
	var status_ uint64
	var serial string
	var ip_address string
	var location string
	var latest_version string
	var registered_time string
	var ip_module_mac string

	query := fmt.Sprintf(`
		SELECT uuid, settop_uuid, status, serial, ip_address, location, latest_version, registered_time, mac 
		FROM sensor 
		WHERE settop_uuid = '%s'
		`, in.GetSettopUuid())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()
	var thresholds []*pb.Threshold

	for rows.Next() {

		err := rows.Scan(&uuid, &settop_uuid, &status_, &serial, &ip_address, &location, &latest_version, &registered_time, &ip_module_mac)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}

		query := fmt.Sprintf(`
		SELECT temp_warning1, temp_danger1, temp_warning2, temp_danger2, temp_warning3,
			   temp_danger3, temp_warning4, temp_danger4, temp_warning5, temp_danger5,
			   temp_warning6, temp_danger6, temp_warning7, temp_danger7, temp_warning8,
			   temp_danger8, temp_warning9, temp_danger9
		FROM threshold 
		WHERE sensor_uuid = '%s'
		`,
			uuid)

		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
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
				err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
				return nil, err
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
		}

		sensorList := &pb.Sensor{}
		sensorList.Uuid = uuid
		sensorList.SettopUuid = settop_uuid
		sensorList.Status = pb.SensorStatus(status_)
		sensorList.Serial = serial
		sensorList.IpAddress = ip_address
		sensorList.Location = location
		sensorList.Thresholds = thresholds
		sensorList.LatestVersion = latest_version
		sensorList.RegisteredTime = registered_time
		sensorList.IpModuleMac = ip_module_mac

		response.SensorList = append(response.SensorList, sensorList)
	}
	return response, nil
}

func (s *server) CreateHistory(ctx context.Context, in *pb.CreateHistoryRequest) (*pb.CreateHistoryResponse, error) {
	log.Printf("Received AddHistory: %s, %s, %f, %f, %s",
		in.History.GetUuid(), in.History.GetSensorSerial(), in.History.GetMinTemp(), in.History.GetMaxTemp(), in.History.GetDate())
	var uuid = uuid.New()

	query := fmt.Sprintf(`
		INSERT INTO history SET
			uuid = '%s', 
			sensor_serial = '%s',
			min_temp = '%f',
			max_temp = '%f',
			date = '%s'
		`,
		uuid.String(), in.History.GetSensorSerial(), in.History.GetMinTemp(), in.History.GetMaxTemp(), in.History.GetDate())

	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer sqlAddRegisterer.Close()

	return &pb.CreateHistoryResponse{}, nil
}

func (s *server) DeleteHistory(ctx context.Context, in *pb.DeleteHistoryRequest) (*pb.DeleteHistoryResponse, error) {
	log.Printf("Received DeleteHistory: %s", in.GetHistoryUuid())

	query := fmt.Sprintf(`
		DELETE FROM history
		WHERE uuid = '%s'
		`,
		in.GetHistoryUuid())

	sqlDeleteRegisterer, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeleteRegisterer.RowsAffected()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	fmt.Println("delete count : ", nRow)
	return &pb.DeleteHistoryResponse{}, nil
}

func (s *server) ReadHistory(ctx context.Context, in *pb.ReadHistoryRequest) (*pb.ReadHistoryResponse, error) {
	log.Printf("Received GetHistory: %s", in.GetHistoryUuid())
	response := &pb.ReadHistoryResponse{}

	var uuid string
	var sensor_serial string
	var min_temp float32
	var max_temp float32
	var date string

	query := fmt.Sprintf(`
		SELECT uuid, sensor_serial, min_temp, max_temp, date  
		FROM history 
		WHERE uuid = '%s'
		`,
		in.GetHistoryUuid())

	rows, err := db.Query(query)

	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &sensor_serial, &min_temp, &max_temp, &date)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan history row: %v", err)
		}

		history := &pb.History{}
		history.Uuid = uuid
		history.SensorSerial = sensor_serial
		history.MinTemp = min_temp
		history.MaxTemp = max_temp
		history.Date = date

		response.History = history
	}

	return response, nil
}

func (s *server) ReadHistoryList(ctx context.Context, in *pb.ReadHistoryListRequest) (*pb.ReadHistoryListResponse, error) {
	log.Printf("Received GetHistoryList: success")
	response := &pb.ReadHistoryListResponse{}

	var uuid string
	var sensor_serial string
	var min_temp float32
	var max_temp float32
	var date string

	query := ""
	trim_interval := 1
	if in.GetInterval() < 10 {
		query = fmt.Sprintf(`
		SELECT uuid, sensor_serial, min_temp, max_temp, date  
		FROM history 
		where date >= '%s' AND date < '%s' AND
		sensor_serial = '%s'
		ORDER by date desc
		LIMIT %d, %d
	`, in.GetPrevDate(), in.GetNextDate(), in.GetSensorSerial(), in.GetCursor(), in.GetCount())
	} else if in.GetInterval() >= 10 && in.GetInterval() < 300 {
		if in.GetInterval() >= 10 && in.GetInterval() < 20 {
			trim_interval = 10
		} else if in.GetInterval() >= 20 && in.GetInterval() < 40 {
			trim_interval = 30
		} else {
			trim_interval = 60
		}
		query = fmt.Sprintf(`
		SELECT uuid, sensor_serial, min_temp, max_temp, date  
		FROM history 
		where date >= '%s' AND date < '%s' AND
		sensor_serial = '%s'
		GROUP by DATE(date), HOUR(date), MINUTE(date), FLOOR(SECOND(date)/%d)
		ORDER by date desc
		LIMIT %d, %d
	`, in.GetPrevDate(), in.GetNextDate(), in.GetSensorSerial(), trim_interval, in.GetCursor(), in.GetCount())
	} else {
		if in.GetInterval() >= (30*10) && in.GetInterval() < (60*20) {
			trim_interval = 10
		} else if in.GetInterval() >= (60*20) && in.GetInterval() < (60*40) {
			trim_interval = 30
		} else {
			trim_interval = 60
		}
		query = fmt.Sprintf(`
			SELECT uuid, sensor_serial, min_temp, max_temp, date  
			FROM history 
			where date >= '%s' AND date < '%s' AND
			sensor_serial = '%s'
			GROUP by DATE(date), HOUR(date), FLOOR(MINUTE(date)/%d)
			ORDER by date desc
			LIMIT %d, %d
		`, in.GetPrevDate(), in.GetNextDate(), in.GetSensorSerial(), trim_interval, in.GetCursor(), in.GetCount())
	}

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)

		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&uuid, &sensor_serial, &min_temp, &max_temp, &date)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan history list row: %v", err)
		}

		historyList := &pb.History{}
		historyList.Uuid = uuid
		historyList.SensorSerial = sensor_serial
		historyList.MinTemp = min_temp
		historyList.MaxTemp = max_temp
		historyList.Date = date

		response.HistoryList = append(response.HistoryList, historyList)
	}
	return response, nil
}

func (s *server) CreateGroup(ctx context.Context, in *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
	log.Printf("Received AddGroup: %s, %s, %d, %s",
		in.Group.GetUuid(), in.Group.GetPlaceUuid(), in.Group.GetGroupId(), in.Group.GetName())
	var uuid = uuid.New()
	response := &pb.CreateGroupResponse{}

	query := fmt.Sprintf(`
		INSERT INTO group_ SET
			uuid = '%s', 
			place_uuid = '%s',
			group_id = '%d',
			name = '%s'
		`,
		uuid.String(), in.Group.GetPlaceUuid(), in.Group.GetGroupId(), in.Group.GetName())

	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	response.Uuid = uuid.String()
	defer sqlAddRegisterer.Close()

	return response, nil
}

func (s *server) UpdateGroup(ctx context.Context, in *pb.UpdateGroupRequest) (*pb.UpdateGroupResponse, error) {
	log.Printf("Received UpdateGroup: %s", in.Group.GetUuid())
	query := fmt.Sprintf(`
		UPDATE group_ SET
			place_uuid = '%s',
			group_id = '%d',
			name = '%s'
		WHERE uuid = '%s'
		`,
		in.Group.GetPlaceUuid(), in.Group.GetGroupId(), in.Group.GetName(), in.Group.GetUuid())

	sqlUpdateRegisterer, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	affectedCount, err := sqlUpdateRegisterer.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		return nil, err
	}
	log.Println("update group complete: ", affectedCount)

	return &pb.UpdateGroupResponse{}, nil
}

func (s *server) DeleteGroup(ctx context.Context, in *pb.DeleteGroupRequest) (*pb.DeleteGroupResponse, error) {
	log.Printf("Received DeleteGroup: %s", in.GetGroupUuid())

	query := fmt.Sprintf(`
		DELETE FROM group_
		WHERE uuid = '%s'
		`,
		in.GetGroupUuid())

	sqlDeleteRegisterer, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeleteRegisterer.RowsAffected()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	fmt.Println("delete count : ", nRow)
	return &pb.DeleteGroupResponse{}, nil
}

func (s *server) ReadGroup(ctx context.Context, in *pb.ReadGroupRequest) (*pb.ReadGroupResponse, error) {
	log.Printf("Received GetGroup: %s", in.GetGroupUuid())
	response := &pb.ReadGroupResponse{}

	var uuid string
	var place_uuid string
	var group_id uint64
	var name string

	query := fmt.Sprintf(`
		SELECT uuid, place_uuid, group_id, name 
		FROM group_ 
		WHERE uuid = '%s'
		`,
		in.GetGroupUuid())

	rows, err := db.Query(query)

	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &place_uuid, &group_id, &name)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		group := &pb.Group{}
		group.Uuid = uuid
		group.PlaceUuid = place_uuid
		group.GroupId = group_id
		group.Name = name

		response.Group = group
	}

	return response, nil
}

func (s *server) ReadGroupList(ctx context.Context, in *pb.ReadGroupListRequest) (*pb.ReadGroupListResponse, error) {
	log.Printf("Received GetGroupList: success")
	response := &pb.ReadGroupListResponse{}

	var uuid string
	var place_uuid string
	var group_id uint64
	var name string

	query := fmt.Sprintf(`
		SELECT uuid, place_uuid, group_id, name 
		FROM group_ 
		WHERE place_uuid = '%s'
		`, in.GetPlaceUuid())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &place_uuid, &group_id, &name)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		groupList := &pb.Group{}
		groupList.Uuid = uuid
		groupList.PlaceUuid = place_uuid
		groupList.GroupId = group_id
		groupList.Name = name

		response.GroupList = append(response.GroupList, groupList)
	}
	return response, nil
}

func (s *server) CreatePermission(ctx context.Context, in *pb.CreatePermissionRequest) (*pb.CreatePermissionResponse, error) {
	log.Printf("Received AddPermission: %s, %s",
		in.Permission.GetUuid(), in.Permission.GetName())
	var uuid = uuid.New()
	response := &pb.CreatePermissionResponse{}

	query := fmt.Sprintf(`
		INSERT INTO permission SET
			uuid = '%s', 
			name = '%s',
			user = '%d',
			permission = '%d',
			sensor_create = '%d',
			sensor_info = '%d',
			ip_module = '%d',
			threshold = '%d',
			sensor_history = '%d'
		`,
		uuid.String(), in.Permission.GetName(), boolToInt(in.Permission.GetUser()), boolToInt(in.Permission.GetPermission()), boolToInt(in.Permission.GetSensorCreate()),
		boolToInt(in.Permission.GetSensorInfo()), boolToInt(in.Permission.GetIpModule()), boolToInt(in.Permission.GetThreshold()), boolToInt(in.Permission.GetSensorHistory()))

	sqlAddPermission, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)

		return nil, err
	}
	defer sqlAddPermission.Close()

	response.Uuid = uuid.String()

	return response, nil
}

func (s *server) UpdatePermission(ctx context.Context, in *pb.UpdatePermissionRequest) (*pb.UpdatePermissionResponse, error) {
	permission := s.getPermission(ctx)
	if !permission.Permission {
		log.Println("err permission")
		return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
	}

	query1 := `
		SELECT uuid, name
		FROM permission
	`
	rows1, err := db.Query(query1)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)

		return nil, err
	}
	defer rows1.Close()

	var default_uuid string
	for rows1.Next() {
		var uuid string
		var name string

		err := rows1.Scan(&uuid, &name)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan permission row: %v", err)
		}
		if name == "default" {
			default_uuid = uuid
		}
	}

	if in.Permission.GetUuid() == default_uuid {
		if in.Permission.GetName() != "default" {
			return nil, status.Errorf(codes.InvalidArgument, "Bad Request : default")
		}
	}

	log.Printf("Received UpdatePermission: %s", in.Permission.GetUuid())
	query := fmt.Sprintf(`
		UPDATE permission SET
			name = '%s',
			user = '%d',
			permission = '%d',
			sensor_create = '%d',
			sensor_info = '%d',
			ip_module = '%d',
			threshold = '%d',
			sensor_history = '%d'
		WHERE uuid = '%s'
		`,
		in.Permission.GetName(), boolToInt(in.Permission.GetUser()), boolToInt(in.Permission.GetPermission()), boolToInt(in.Permission.GetSensorCreate()),
		boolToInt(in.Permission.GetSensorInfo()), boolToInt(in.Permission.GetIpModule()), boolToInt(in.Permission.GetThreshold()), boolToInt(in.Permission.GetSensorHistory()), in.Permission.GetUuid())

	sqlUpdatePermission, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		return nil, status.Errorf(codes.Internal, "Failed to update permission: %v", err)
	}
	affectedCount, err := sqlUpdatePermission.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)

		return nil, err
	}
	log.Println("update permission complete: ", affectedCount)

	return &pb.UpdatePermissionResponse{}, nil
}

func (s *server) DeletePermission(ctx context.Context, in *pb.DeletePermissionRequest) (*pb.DeletePermissionResponse, error) {
	log.Printf("Received DeletePermission: %s", in.GetPermissionUuid())

	query1 := `
		SELECT uuid, name
		FROM permission
	`
	rows1, err := db.Query(query1)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)

		return nil, err
	}
	defer rows1.Close()

	var default_uuid string
	for rows1.Next() {
		var uuid string
		var name string

		err := rows1.Scan(&uuid, &name)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan permission row: %v", err)
		}
		if name == "default" {
			default_uuid = uuid
		}
	}

	if in.GetPermissionUuid() == default_uuid {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request : default")
	}
	query := fmt.Sprintf(`
		DELETE FROM permission
		WHERE uuid = '%s'
		`,
		in.GetPermissionUuid())

	sqlDeletePermission, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)

		return nil, err
	}

	nRow, err := sqlDeletePermission.RowsAffected()
	if err != nil {
		log.Println(err)
		return nil, status.Errorf(codes.Internal, "Failed to get affected count after delete: %v", err)
	}
	log.Println("delete permission complete, affected rows: ", nRow)
	return &pb.DeletePermissionResponse{}, nil
}

func (s *server) ReadPermissionList(ctx context.Context, in *pb.ReadPermissionListRequest) (*pb.ReadPermissionListResponse, error) {
	log.Printf("Received GetPermissionList: success")
	response := &pb.ReadPermissionListResponse{}

	query := `
		SELECT uuid, name, user, permission, sensor_create, sensor_info, ip_module, threshold, sensor_history  
		FROM permission
	`

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)

		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var uuid string
		var name string
		var user uint64
		var permission uint64
		var sensor_create uint64
		var sensor_info uint64
		var ip_module uint64
		var threshold uint64
		var sensor_history uint64

		err := rows.Scan(&uuid, &name, &user, &permission, &sensor_create, &sensor_info, &ip_module, &threshold, &sensor_history)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan permission row: %v", err)
		}

		permissionList := &pb.Permission{
			Uuid:          uuid,
			Name:          name,
			User:          intToBool(user),
			Permission:    intToBool(permission),
			SensorCreate:  intToBool(sensor_create),
			SensorInfo:    intToBool(sensor_info),
			IpModule:      intToBool(ip_module),
			Threshold:     intToBool(threshold),
			SensorHistory: intToBool(sensor_history),
		}

		response.PermissionList = append(response.PermissionList, permissionList)
	}

	return response, nil
}

func (s *server) FindEmail(ctx context.Context, in *pb.FindEmailRequest) (*pb.FindEmailResponse, error) {
	log.Printf("Find Email")

	response := &pb.FindEmailResponse{}
	var auth_email string

	var query string
	if in.GetCompanyName() == "" && in.GetCompanyNumber() == "" {
		query = fmt.Sprintf(`
		SELECT auth_email
		FROM registerer 
		WHERE name = '%s'
		`, in.GetName())
	} else {
		query = fmt.Sprintf(`
		SELECT auth_email
		FROM registerer 
		WHERE name = '%s' AND company_name = '%s' AND company_number = '%s'
		`,
			in.GetName(), in.GetCompanyName(), in.GetCompanyNumber())
	}

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&auth_email)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan email row: %v", err)
		}

		response.Email = auth_email
	}
	if response.Email == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
	}
	return response, nil
}

func (s *server) MainList(ctx context.Context, in *pb.MainListRequest) (*pb.MainListResponse, error) {
	response, ok := mainListMapping.GetMapping(in.GetRegistererUuid())
	if ok {
		fmt.Println("MainListResponse found")
		return response, nil
	} else {
		fmt.Println("MainListResponse not found.")
		response := &pb.MainListResponse{}

		// Place List
		placeResponse, err := s.ReadPlaceList(ctx, &pb.ReadPlaceListRequest{})
		if err != nil {
			log.Println(err)
			// You can customize the error response and status code based on your application's requirements.
			return nil, status.Errorf(codes.Internal, "Failed to retrieve place list: %v", err)
		}

		var settopList []*pb.Settop
		var sensorList []*pb.Sensor

		// Iterate through each place to get settops and sensors

		// Settop List for the current place
		settopResponse, err := s.ReadSettopList(ctx, &pb.ReadSettopListRequest{})
		if err != nil {
			log.Println(err)
			// You can customize the error response and status code based on your application's requirements.
			return nil, status.Errorf(codes.Internal, "Failed to retrieve settop list: %v", err)
		}

		// Iterate through each settop to get sensors
		for _, settop := range settopResponse.SettopList {
			// Sensor List for the current settop
			sensorResponse, err := s.ReadSensorList(ctx, &pb.ReadSensorListRequest{
				SettopUuid: settop.GetUuid(),
			})
			if err != nil {
				log.Println(err)
				// You can customize the error response and status code based on your application's requirements.
				return nil, status.Errorf(codes.Internal, "Failed to retrieve sensor list: %v", err)
			}
			settop_ := &pb.Settop{}
			settop_.Uuid = settop.GetUuid()
			settop_.PlaceUuid = settop.GetPlaceUuid()
			settop_.Serial = settop.GetSerial()
			settop_.Room = settop.GetRoom()
			settop_.Floor = settop.GetFloor()
			settop_.Mac1 = settop.GetMac1()
			settop_.Mac2 = settop.GetMac2()
			settop_.IsAlive = settop.GetIsAlive()
			settop_.LatestVersion = settop.GetLatestVersion()
			settop_.RegisteredTime = settop.GetRegisteredTime()
			settopList = append(settopList, settop_)

			// Add sensors to the list
			sensorList = append(sensorList, sensorResponse.GetSensorList()...)
		}

		response.PlaceList = placeResponse.GetPlaceList()
		response.SettopList = settopList
		response.SensorList = sensorList
		mainListMapping.AddMapping(in.GetRegistererUuid(), response)
		return response, nil
	}

}

func (s *server) StreamImage(req *pb.ImageRequest, stream pb.MainControl_StreamImageServer) error {

	sensorSerial := req.SensorSerial
	date := req.Date
	requestTime, err := time.Parse("2006-01-02 15:04:05", date)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Invalid date format: %v", err)
	}
	// folderPath := filepath.Join("/appserver/storage_data", sensorSerial, requestTime.Format("2006-01-02"))
	folderPath := filepath.Join("/Users/bkpark/works/go/trusafer/main_server/storage_data", sensorSerial, requestTime.Format("2006-01-02"))
	fileName := requestTime.Format("15:04:05") + ".jpg"
	filePath := filepath.Join(folderPath, fileName)
	log.Println("stream image called: serial =", sensorSerial, "date =", date)

	imageFile, err := os.Open(filePath)
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
	}
	defer imageFile.Close()

	buffer := make([]byte, 2048)
	for {
		n, err := imageFile.Read(buffer)
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalf("Error reading image file: %v", err)
			return err
		}

		if err := stream.Send(&pb.ImageChunk{TempImage: buffer[:n]}); err != nil {
			log.Fatalf("Error sending image chunk: %v", err)
			return err
		}
	}
	return nil
}

func (s *server) SubscribeFirebase(ctx context.Context, in *pb.SubscribeFirebaseRequest) (*pb.SubscribeFirebaseResponse, error) {
	log.Printf("SubscribeFirebase called ", in.GetToken(), "IsSubscribe = ", in.GetIsSubscribe())
	response := &pb.SubscribeFirebaseResponse{}
	requestData := make(map[string]string)
	requestData["token"] = in.GetToken()
	tokens := []string{requestData["token"]}
	topic := in.GetTopic()

	err := firebaseutil.SubscribeToTopic(tokens, topic, in.GetIsSubscribe())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
	}
	return response, nil
}

func (s *server) LogList(ctx context.Context, in *pb.LogListRequest) (*pb.LogListResponse, error) {
	log.Printf("LogList called")
	response := &pb.LogListResponse{}

	query := ""

	if in.GetUnit() == "mobile" {
		query = fmt.Sprintf(`
		SELECT uuid, unit, message, registered_time 
		FROM log 
		WHERE unit = '%s' 
		ORDER by registered_time desc 
		LIMIT %d, %d 
	`, in.GetUnit(), in.GetCursor(), in.GetCount())
	} else {
		query = fmt.Sprintf(`
		SELECT uuid, unit, message, registered_time 
		FROM log 
		WHERE unit = '%s' AND sensor_serial = '%s'
		ORDER by registered_time desc 
		LIMIT %d, %d 
	`, in.GetUnit(), in.GetSensorSerial(), in.GetCursor(), in.GetCount())
	}

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var uuid string
		var unit string
		var message string
		var registered_time string
		err := rows.Scan(&uuid, &unit, &message, &registered_time)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan log row: %v", err)
		}

		logList := &pb.Log{
			Uuid:           uuid,
			Unit:           unit,
			Message:        message,
			RegisteredTime: registered_time,
		}

		response.Log = append(response.Log, logList)
	}
	return response, nil
}

// func (s *server) UploadCompanyImage(ctx context.Context, in *pb.UploadCompanyImageRequest) (*pb.UploadCompanyImageResponse, error) {
// 	log.Printf("UploadCompanyImage called")
// 	response := &pb.UploadCompanyImageResponse{}

// 	md, ok := metadata.FromIncomingContext(ctx)
// 	if !ok {
// 		return nil, status.Errorf(codes.Unauthenticated, "not read metadata")
// 	}
// 	if md["authorization"] == nil {
// 		return nil, status.Errorf(codes.Unauthenticated, "not read metadata")
// 	}
// 	authHeaders, ok := md["authorization"]
// 	if !ok || len(authHeaders) == 0 {
// 		return nil, status.Errorf(codes.Unauthenticated, "Authentication token not provided")
// 	}
// 	token := authHeaders[0]
// 	claims := &Claims{}
// 	_, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
// 		return []byte("secret2"), nil
// 	})
// 	if err != nil {
// 		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
// 	}
// 	switch in.Method {
// 	case "POST":
// 		in.ParseMultipartForm(10 << 20) //10 MB
// 		file, handler, err := in.FormFile("file_image")
// 		if err != nil {
// 			log.Println("error retrieving file", err)
// 			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
// 			return nil, err
// 		}
// 		defer file.Close()
// 		dst, err := os.Create(handler.Filename)
// 		if err != nil {
// 			log.Println("error creating file", err)
// 			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
// 			return nil, err
// 		}
// 		defer dst.Close()
// 		if _, err := io.Copy(dst, file); err != nil {
// 			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
// 			return nil, err
// 		}
// 	}
// 	return response, nil
// }

// func (s *server) ReadCompanyImage(ctx context.Context, in *pb.ReadCompanyImageRequest) (*pb.ReadCompanyImageResponse, error) {
// 	log.Printf("ReadCompanyImage called")
// 	response := &pb.ReadCompanyImageResponse{}

// 	md, ok := metadata.FromIncomingContext(ctx)
// 	if !ok {
// 		return nil, status.Errorf(codes.Unauthenticated, "not read metadata")
// 	}
// 	if md["authorization"] == nil {
// 		return nil, status.Errorf(codes.Unauthenticated, "not read metadata")
// 	}
// 	authHeaders, ok := md["authorization"]
// 	if !ok || len(authHeaders) == 0 {
// 		return nil, status.Errorf(codes.Unauthenticated, "Authentication token not provided")
// 	}
// 	token := authHeaders[0]
// 	claims := &Claims{}
// 	_, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
// 		return []byte("secret2"), nil
// 	})
// 	if err != nil {
// 		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
// 	}
// 	filePath := "/Users/bkpark/works/go/trusafer/main_server/20230926_185142.jpg"

// 	file, err := os.Open(filePath)
// 	if err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	defer file.Close()

// 	w.Header().Set("Content-Type", "image/jpeg")
// 	if _, err := io.Copy(w, file); err != nil {
// 		http.Error(w, err.Error(), http.StatusInternalServerError)
// 		return
// 	}
// 	return response, nil
// }

func roundToDecimalPlaces(value float32, decimalPlaces int) float32 {
	shift := math.Pow(10, float64(decimalPlaces))
	return float32(math.Round(float64(value)*shift) / shift)
}
