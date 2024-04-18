package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"fmt"
	"log"
	"math"
	"reflect"
	"sort"
	"strings"
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
type RegistererInfoMapping struct {
	mu      sync.Mutex
	mapping map[string][]*pb.RegistererInfo
}

func NewRegistererInfoMapping() *RegistererInfoMapping {
	return &RegistererInfoMapping{
		mapping: make(map[string][]*pb.RegistererInfo),
	}
}

func (m *RegistererInfoMapping) AddRegistererInfodMapping(registererUUID string, response []*pb.RegistererInfo) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mapping[registererUUID] = response
}

func (m *RegistererInfoMapping) GetRegistererInfoMapping(registererUUID string) ([]*pb.RegistererInfo, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	response, ok := m.mapping[registererUUID]
	return response, ok
}

func (m *RegistererInfoMapping) RemoveRegistererInfoMapping(registererUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mapping, registererUUID)
}

func DeleteRegistererInfo(s *server, ctx context.Context) {
	registererinfo, err := s.ReadRegisterer(ctx, &pb.ReadRegistererRequest{
		Name: "check",
	})
	if err != nil {
		log.Println(err)
	}
	defer registererInfoMapping.RemoveRegistererInfoMapping(registererinfo.GetRegistererInfo().GetUuid())
}

var registererInfoMapping = NewRegistererInfoMapping()

// ============================MainList Mapping====================================================

type ThresholdMapping struct {
	mu      sync.Mutex
	mapping map[string][]*pb.Threshold
}

// ============================Threshold Mapping====================================================

func NewThresholdMapping() *ThresholdMapping {
	return &ThresholdMapping{
		mapping: make(map[string][]*pb.Threshold),
	}
}

func (m *ThresholdMapping) AddThresholdMapping(registererUUID string, response []*pb.Threshold) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mapping[registererUUID] = response
}

func (m *ThresholdMapping) GetThresholdMapping(registererUUID string) ([]*pb.Threshold, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	response, ok := m.mapping[registererUUID]
	return response, ok
}

func (m *ThresholdMapping) RemoveThresholdMapping(registererUUID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.mapping, registererUUID)
}

func DeleteThreshold(s *server, ctx context.Context) {
	registererinfo, err := s.ReadRegisterer(ctx, &pb.ReadRegistererRequest{
		Name: "check",
	})
	if err != nil {
		log.Println(err)
	}
	defer thresholdMapping.RemoveThresholdMapping(registererinfo.GetRegistererInfo().GetUuid())
}

var thresholdMapping = NewThresholdMapping()

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
		return []byte(string(Conf.Jwt.SecretKeyAT)), nil
	})
	if err != nil {
		log.Println(err)
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
	permission.SettopCreate = registererinfo.GetRegistererInfo().GetPSettopCreate()
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
	newUUID := uuid.String()
	var first_permission_uuid string
	if in.Registerer.GetPermissionUuid() == "" {
		defaultPermissionQuery := fmt.Sprintf(`
		SELECT uuid 
		FROM permission 
		WHERE name = '%s'
		`, "default")
		rows, err := db.Query(defaultPermissionQuery)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to fetch default permission UUID: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&first_permission_uuid)
			if err != nil {
				log.Println(err)
				return nil, status.Errorf(codes.Internal, "Failed to scan default permission rows: %v", err)
			}
		}
	} else {
		first_permission_uuid = in.Registerer.GetPermissionUuid()
	}
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
		newUUID, in.Registerer.GetAuthEmail(), in.Registerer.GetCompanyName(),
		in.Registerer.GetCompanyNumber(), in.Registerer.GetStatus(), boolToInt(in.Registerer.GetIsAlarm()),
		first_permission_uuid, in.Registerer.GetName())
	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Not Found Data: %v", err)
		return nil, err
	}
	defer sqlAddRegisterer.Close()
	var uuid_ string
	var auth_email string
	var company_name string
	var company_number string
	var status_ pb.RegistererStatus
	var is_alarm uint64
	var permission_uuid string
	var name string
	query = fmt.Sprintf(`
		SELECT uuid, auth_email, company_name, company_number, status, is_alarm, permission_uuid, name  
		FROM registerer 
		WHERE uuid = '%s'
		`,
		newUUID)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, status.Errorf(codes.Internal, "Failed to fetch registerer information: %v", err)
	}
	defer rows.Close()
	response := &pb.CreateRegistererResponse{}
	registererList := &pb.Registerer{}
	for rows.Next() {
		err := rows.Scan(&uuid_, &auth_email, &company_name, &company_number, &status_, &is_alarm, &permission_uuid, &name)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan registerer rows: %v", err)
		}
		registererList.Uuid = uuid_
		registererList.AuthEmail = auth_email
		registererList.CompanyName = company_name
		registererList.CompanyNumber = company_number
		registererList.Status = status_
		registererList.IsAlarm = intToBool(is_alarm)
		registererList.PermissionUuid = permission_uuid
		registererList.Name = name
	}
	response.Registerer = registererList
	var group_default_uuid string
	query = fmt.Sprintf(`
		SELECT uuid 
		FROM group_ 
		WHERE name = 'default'
	`)
	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&group_default_uuid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	query = fmt.Sprintf(`
		INSERT INTO group_gateway (group_uuid, registerer_uuid)
		VALUES ('%s', '%s')`,
		group_default_uuid, uuid_)
	sqlAddRegisterer, err = db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer sqlAddRegisterer.Close()
	return response, nil
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

	_, err = db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
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
		log.Println("not read metadata")
		return nil, status.Errorf(codes.Unauthenticated, "not read metadata")
	}
	if md["authorization"] == nil {
		log.Println("not read metadata")
		return nil, status.Errorf(codes.Unauthenticated, "not read metadata")
	}
	authHeaders, ok := md["authorization"]
	if !ok || len(authHeaders) == 0 {
		log.Println("Authentication token not provided")
		return nil, status.Errorf(codes.Unauthenticated, "Authentication token not provided")
	}
	token := authHeaders[0]
	claims := &Claims{}
	_, err := jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(string(Conf.Jwt.SecretKeyAT)), nil
	})
	if err != nil {
		log.Println("Invalid authentication token")
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
	var settop_create uint64
	var sensor_info uint64
	var ip_module uint64
	var threshold uint64
	var sensor_history uint64
	var group_uuid string
	var group_uuids []string

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

		query := fmt.Sprintf(`
			SELECT group_uuid 
			FROM group_gateway 
			WHERE registerer_uuid = '%s'
		`, uuid_)
		rows1, _ := db.Query(query)
		defer rows1.Close()
		for rows1.Next() {
			err := rows1.Scan(&group_uuid)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			group_uuids = append(group_uuids, group_uuid)
		}
		if len(group_uuids) == 0 {
			var group_default_uuid string
			query := fmt.Sprintf(`
			SELECT uuid 
			FROM group_ 
			WHERE name = 'default'
		`)
			rows, err := db.Query(query)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(&group_default_uuid)
				if err != nil {
					log.Println(err)
					return nil, err
				}
			}
			query = fmt.Sprintf(`
            INSERT INTO group_gateway (group_uuid, registerer_uuid)
            VALUES ('%s', '%s')`,
				group_default_uuid, uuid_)
			sqlAddRegisterer, err := db.Query(query)
			if err != nil {
				log.Println(err)
				err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
				return nil, err
			}
			defer sqlAddRegisterer.Close()
		}
		if permission_uuid != "" {
			permissionQuery := fmt.Sprintf(`
				SELECT user, permission, settop_create, sensor_info, ip_module, threshold, sensor_history  
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
				err := permissionRows.Scan(&user, &permission, &settop_create, &sensor_info, &ip_module, &threshold, &sensor_history)
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
			PSettopCreate:  intToBool(settop_create),
			PSensorInfo:    intToBool(sensor_info),
			PIpModule:      intToBool(ip_module),
			PThreshold:     intToBool(threshold),
			PSensorHistory: intToBool(sensor_history),
			GroupUuid:      group_uuids,
		}
		response.RegistererInfo = registerer
	}
	if response.RegistererInfo == nil {
		var firstPermissionUUID string
		var permission_name string
		var user_status int
		if claims.Admin {
			permission_name = "master"
			user_status = 1
		} else {
			permission_name = "default"
			user_status = 2
		}
		defaultPermissionQuery := fmt.Sprintf(`
			SELECT uuid 
			FROM permission 
			WHERE name = '%s'
			`, permission_name)
		rows, err := db.Query(defaultPermissionQuery)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to fetch default permission UUID: %v", err)
		}
		defer rows.Close()
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
			genUUID, claims.Email, "", "", user_status, 1, firstPermissionUUID, in.GetName())
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
		var group_default_uuid string
		query = fmt.Sprintf(`
			SELECT uuid 
			FROM group_ 
			WHERE name = 'default'
		`)
		rows, err = db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&group_default_uuid)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}
		query = fmt.Sprintf(`
            INSERT INTO group_gateway (group_uuid, registerer_uuid)
            VALUES ('%s', '%s')`,
			group_default_uuid, genUUID)

		sqlAddRegisterer, err = db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer sqlAddRegisterer.Close()
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
	permission := s.getPermission(ctx)
	if !permission.SettopCreate {
		log.Println("err permission")
		return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
	}
	log.Printf("Received AddSettop: %s, %s, %s, %s, %s, %s",
		in.Settop.GetUuid(), in.Settop.GetPlaceUuid(), in.Settop.GetSerial(),
		in.Settop.GetRoom(), in.Settop.GetFloor(), in.Settop.GetRegisteredTime())
	var uuid = uuid.New()
	response := &pb.CreateSettopResponse{}
	isalive := 0
	if !in.Settop.GetIsAlive() {
		isalive = 0
	} else {
		isalive = 1
	}
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
			fw_version = '%s',
			registered_time = '%s'
		`,
		uuid.String(), in.Settop.GetPlaceUuid(), in.Settop.GetSerial(),
		in.Settop.GetRoom(), in.Settop.GetFloor(), in.Settop.GetMac1(),
		in.Settop.GetMac2(), isalive, in.Settop.GetFwVersion(), in.Settop.GetRegisteredTime())
	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	response.Uuid = uuid.String()
	defer sqlAddRegisterer.Close()
	if len(in.GetGroupUuid()) > 0 {
		for _, group_uuid := range in.GetGroupUuid() {
			query := fmt.Sprintf(`
				INSERT INTO group_gateway (group_uuid, settop_uuid)
				VALUES ('%s', '%s')`,
				group_uuid, uuid.String())
			sqlAddRegisterer, err := db.Query(query)
			if err != nil {
				log.Println(err)
				err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
				return nil, err
			}
			defer sqlAddRegisterer.Close()
		}
	} else {
		var group_default_uuid string
		query = fmt.Sprintf(`
			SELECT uuid 
			FROM group_ 
			WHERE name = 'default'
		`)
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&group_default_uuid)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}
		query := fmt.Sprintf(`
				INSERT INTO group_gateway (group_uuid, settop_uuid)
				VALUES ('%s', '%s')`,
			group_default_uuid, uuid.String())
		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer sqlAddRegisterer.Close()
	}
	return response, nil
}

func (s *server) UpdateSettop(ctx context.Context, in *pb.UpdateSettopRequest) (*pb.UpdateSettopResponse, error) {
	DeleteMainList(s, ctx)
	permission := s.getPermission(ctx)
	if !permission.SettopCreate {
		log.Println("err permission")
		return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
	}
	var fwVersion string
	if !permission.IpModule {
		query := fmt.Sprintf(`
        SELECT fw_version
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
			err := rows.Scan(&fwVersion)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}
		if fwVersion != in.Settop.FwVersion {
			log.Println("err permission")
			return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
		}
	}
	log.Printf("Received UpdateSettop: %s", in.Settop.GetUuid())
	isalive := 0
	if !in.Settop.GetIsAlive() {
		isalive = 0
	} else {
		isalive = 1
	}
	query := fmt.Sprintf(`
		UPDATE settop SET
			place_uuid = '%s', 
			serial = '%s',
			room = '%s',
			floor = '%s',
			mac1 = '%s',
			mac2 = '%s',
			is_alive = '%d',
			fw_version = '%s',
			registered_time = '%s'
		WHERE uuid = '%s'
		`,
		in.Settop.GetPlaceUuid(), in.Settop.GetSerial(),
		in.Settop.GetRoom(), in.Settop.GetFloor(), in.Settop.GetMac1(), in.Settop.GetMac2(),
		isalive, in.Settop.GetFwVersion(), in.Settop.GetRegisteredTime(), in.Settop.GetUuid())
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
	permission := s.getPermission(ctx)
	if !permission.SettopCreate {
		log.Println("err permission")
		return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
	}
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
	var fw_version string
	var registered_time string
	query := fmt.Sprintf(`
		SELECT uuid, place_uuid, serial, room, floor, mac1, mac2, is_alive, fw_version, registered_time  
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
		err := rows.Scan(&uuid, &place_uuid, &serial, &room, &floor, &mac1, &mac2, &is_alive, &fw_version, &registered_time)
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
		settop.FwVersion = fw_version
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
	var group_settop_uuid sql.NullString
	var uuid string
	var place_uuid string
	var serial string
	var room string
	var floor string
	var mac1 string
	var mac2 string
	var is_alive bool
	var fw_version string
	var registered_time string
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
		return []byte(string(Conf.Jwt.SecretKeyAT)), nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
	}
	if claims.Admin {
		query := fmt.Sprintf(`
		SELECT uuid, place_uuid, serial, room, floor, mac1, mac2, is_alive, fw_version, registered_time  
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
			err := rows.Scan(&uuid, &place_uuid, &serial, &room, &floor, &mac1, &mac2, &is_alive, &fw_version, &registered_time)
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
			settopList.FwVersion = fw_version
			settopList.RegisteredTime = registered_time
			settopList.PlaceAddress = place_address
			settopList.PlaceName = place_name
			response.SettopList = append(response.SettopList, settopList)
		}
	} else {
		groupSettopUUIDs := make(map[string]bool)
		readRegistererResponse, _ := s.ReadRegisterer(ctx, &pb.ReadRegistererRequest{
			Name: "check",
		})
		for _, group_uuid := range readRegistererResponse.GetRegistererInfo().GetGroupUuid() {
			query := fmt.Sprintf(`
			SELECT settop_uuid  
			FROM group_gateway 
			WHERE group_uuid = '%s'
			`, group_uuid)
			rows, err := db.Query(query)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(&group_settop_uuid)
				if err != nil {
					log.Println(err)
					return nil, err
				}
				if getNullStringValidValue(group_settop_uuid) == "" || groupSettopUUIDs[getNullStringValidValue(group_settop_uuid)] {
					continue
				}
				groupSettopUUIDs[getNullStringValidValue(group_settop_uuid)] = true
				query := fmt.Sprintf(`
				SELECT uuid, place_uuid, serial, room, floor, mac1, mac2, is_alive, fw_version, registered_time  
				FROM settop 
				WHERE uuid = '%s' 
				`, getNullStringValidValue(group_settop_uuid))
				rows, err := db.Query(query)
				if err != nil {
					log.Println(err)
					err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
					return nil, err
				}
				defer rows.Close()
				for rows.Next() {
					err := rows.Scan(&uuid, &place_uuid, &serial, &room, &floor, &mac1, &mac2, &is_alive, &fw_version, &registered_time)
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
					settopList.FwVersion = fw_version
					settopList.RegisteredTime = registered_time
					settopList.PlaceAddress = place_address
					settopList.PlaceName = place_name
					response.SettopList = append(response.SettopList, settopList)
				}
			}
		}
	}
	return response, nil
}

func (s *server) CreateSensor(ctx context.Context, in *pb.CreateSensorRequest) (*pb.CreateSensorResponse, error) {
	DeleteMainList(s, ctx)
	log.Printf("Received AddSensor: %s, %s, %d, %s, %s, %s, %s",
		in.Sensor.GetUuid(), in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(),
		in.Sensor.GetRegisteredTime())
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
			registered_time = '%s',
			mac = '%s',
			name = '%s'
		`,
		uuid.String(), in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(),
		in.Sensor.GetRegisteredTime(), in.Sensor.IpModuleMac, in.Sensor.GetName())
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
	thresholdMapping = NewThresholdMapping()
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
		in.Sensor.GetRegisteredTime(), in.Sensor.GetName())
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
			registered_time = '%s',
			mac = '%s',
			name = '%s'
		WHERE uuid = '%s'
		`,
		in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(),
		in.Sensor.GetRegisteredTime(), in.Sensor.IpModuleMac, in.Sensor.GetName(), in.Sensor.GetUuid())

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
	var registered_time string
	var ip_module_mac string
	var name sql.NullString
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
		SELECT uuid, settop_uuid, status, serial, ip_address, location, registered_time,mac, name 
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
		err := rows.Scan(&uuid, &settop_uuid, &status_, &serial, &ip_address, &location, &registered_time, &ip_module_mac, &name)
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
		sensor.RegisteredTime = registered_time
		sensor.IpModuleMac = ip_module_mac
		sensor.Thresholds = thresholds
		sensor.Name = getNullStringValidValue(name)
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
	var registered_time string
	var ip_module_mac string
	var name sql.NullString
	query := fmt.Sprintf(`
		SELECT uuid, settop_uuid, status, serial, ip_address, location, registered_time, mac, name 
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
		err := rows.Scan(&uuid, &settop_uuid, &status_, &serial, &ip_address, &location, &registered_time, &ip_module_mac, &name)
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
		sensorList.RegisteredTime = registered_time
		sensorList.IpModuleMac = ip_module_mac
		sensorList.Name = getNullStringValidValue(name)
		response.SensorList = append(response.SensorList, sensorList)
	}
	return response, nil
}

func (s *server) ReadHistoryList(ctx context.Context, in *pb.ReadHistoryListRequest) (*pb.ReadHistoryListResponse, error) {
	log.Printf("Received GetHistoryList: called")
	response := &pb.ReadHistoryListResponse{}
	// var min_temp float32
	// var max_temp float32
	// var date string
	query := ""
	trim_interval := 1

	prev_date := in.GetPrevDate()
	next_date := in.GetNextDate()
	location, err := time.LoadLocation("Asia/Seoul")
	prevTime, err := time.Parse("2006-01-02 15:04:05", prev_date)
	nextTime, err := time.Parse("2006-01-02 15:04:05", next_date)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid date format: %v", err)
	}
	prevTime = prevTime.In(location).Add(-9 * time.Hour)
	nextTime = nextTime.In(location).Add(-9 * time.Hour)
	startRFC3339 := prevTime.Format(time.RFC3339)
	stopRFC3339 := nextTime.Format(time.RFC3339)

	if in.GetInterval() < 10 {
		query = fmt.Sprintf(`from(bucket: "%s")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "%s")
		|> filter(fn: (r) => r._field != "image_data")
		`,
			Conf.InfluxDB.Bucket,
			startRFC3339,
			stopRFC3339,
			in.GetSensorSerial())
	} else if in.GetInterval() >= 10 && in.GetInterval() < 300 {
		if in.GetInterval() >= 10 && in.GetInterval() < 20 {
			trim_interval = 10
		} else if in.GetInterval() >= 20 && in.GetInterval() < 40 {
			trim_interval = 30
		} else {
			trim_interval = 60
		}

		query = fmt.Sprintf(`from(bucket: "%s")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "%s")
		|> filter(fn: (r) => r._field != "image_data")
		|> aggregateWindow(every: %ds, fn: max, createEmpty: false)
		|> group(columns: ["_measurement", "_field"])
		`,
			Conf.InfluxDB.Bucket,
			startRFC3339,
			stopRFC3339,
			in.GetSensorSerial(), trim_interval)
	} else {
		if in.GetInterval() >= (30*10) && in.GetInterval() < (60*20) {
			trim_interval = 10
		} else if in.GetInterval() >= (60*20) && in.GetInterval() < (60*40) {
			trim_interval = 30
		} else {
			trim_interval = 60
		}

		query = fmt.Sprintf(`from(bucket: "%s")
		|> range(start: %s, stop: %s)
		|> filter(fn: (r) => r._measurement == "%s")
		|> filter(fn: (r) => r._field != "image_data")
		|> aggregateWindow(every: %dm, fn: max, createEmpty: false)
		|> group(columns: ["_measurement", "_field"])
		`,
			Conf.InfluxDB.Bucket,
			startRFC3339,
			stopRFC3339,
			in.GetSensorSerial(), trim_interval)
	}

	results, err := _queryAPI.Query(context.Background(), query)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
	}
	dataMap := make(map[int64]*pb.History)
	// historyList := &pb.History{}
	for results.Next() {
		if results.Record().Value() == nil {
			continue
		}

		seconds := results.Record().Time().Unix()
		if _, ok := dataMap[seconds]; !ok {
			dataMap[seconds] = &pb.History{}
		}
		data := dataMap[seconds]
		switch results.Record().Field() {
		case "min_value":
			data.MinTemp = float32(results.Record().Value().(float64))
		case "max_value":
			data.MaxTemp = float32(results.Record().Value().(float64))
		}
		t := time.Unix(seconds, 0)
		formattedTime := t.Format("2006-01-02 15:04:05")
		data.Date = formattedTime
	}

	for _, data := range dataMap {
		response.HistoryList = append(response.HistoryList, data)
	}
	less := func(i, j int) bool {
		return response.HistoryList[i].Date < response.HistoryList[j].Date
	}
	sort.Slice(response.HistoryList, less)
	return response, nil
}

func (s *server) CreateGroup(ctx context.Context, in *pb.CreateGroupRequest) (*pb.CreateGroupResponse, error) {
	log.Printf("Received AddGroup: %s, %s",
		in.Group.GetUuid(), in.Group.GetName())
	var uuid = uuid.New()
	response := &pb.CreateGroupResponse{}
	query := fmt.Sprintf(`
		INSERT INTO group_ SET
			uuid = '%s', 
			name = '%s'
		`,
		uuid.String(), in.Group.GetName())
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
	log.Printf("Received UpdateGroup ", in.Group.GetName(), " ", in.Group.GetUuid())
	query := fmt.Sprintf(`
		UPDATE group_ SET
			name = '%s' 
		WHERE uuid = '%s'
		`,
		in.Group.GetName(), in.Group.GetUuid())
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
	log.Printf("Received DeleteGroup")
	var group_name string
	query := fmt.Sprintf(`
	SELECT name 
	FROM group_ 
	WHERE uuid = '%s'
	`, in.GetGroupUuid())
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&group_name)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	if group_name == "default" {
		err = status.Errorf(codes.InvalidArgument, "Bad Request: Default groups cannot be deleted.")
		return nil, err
	}
	query = fmt.Sprintf(`
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
	query = fmt.Sprintf(`
		DELETE FROM group_gateway 
		WHERE group_uuid = '%s'
		`,
		in.GetGroupUuid())
	sqlDeleteRegisterer, err = db.Exec(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
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
	_, err = jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(string(Conf.Jwt.SecretKeyAT)), nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
	}
	var firebase_token string
	query = fmt.Sprintf(`
		SELECT token 
		FROM firebase_token 
		WHERE email = '%s' AND group_uuid = '%s' 
	`, claims.Email, in.GetGroupUuid())
	rows, err = db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&firebase_token)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		requestData := make(map[string]string)
		requestData["token"] = firebase_token
		tokens := []string{requestData["token"]}
		err = firebaseutil.SubscribeToTopic(tokens, in.GetGroupUuid(), false)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		}
		query = fmt.Sprintf(`
			DELETE FROM firebase_token 
			WHERE email = '%s' AND group_uuid = '%s' 
			`,
			claims.Email, in.GetGroupUuid())
		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Not Found Data: %v", err)
			return nil, err
		}
		defer sqlAddRegisterer.Close()
	}
	return &pb.DeleteGroupResponse{}, nil
}

func (s *server) ReadGroup(ctx context.Context, in *pb.ReadGroupRequest) (*pb.ReadGroupResponse, error) {
	log.Printf("Received GetGroup")
	response := &pb.ReadGroupResponse{}
	var uuid string
	var name string
	query := fmt.Sprintf(`
		SELECT uuid, name 
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
		err := rows.Scan(&uuid, &name)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		group := &pb.Group{}
		group.Uuid = uuid
		group.Name = name
		response.Group = group
	}
	return response, nil
}

func (s *server) ReadGroupList(ctx context.Context, in *pb.ReadGroupListRequest) (*pb.ReadGroupListResponse, error) {
	log.Printf("Received GetGroupList: success")
	response := &pb.ReadGroupListResponse{}
	var uuid string
	var name string
	query := fmt.Sprintf(`
		SELECT uuid, name 
		FROM group_ 
		`)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&uuid, &name)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		if name == "master" {
			continue
		}
		groupList := &pb.Group{}
		groupList.Uuid = uuid
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
			settop_create = '%d',
			sensor_info = '%d',
			ip_module = '%d',
			threshold = '%d',
			sensor_history = '%d'
		`,
		uuid.String(), in.Permission.GetName(), boolToInt(in.Permission.GetUser()), boolToInt(in.Permission.GetPermission()), boolToInt(in.Permission.GetSettopCreate()),
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
			settop_create = '%d',
			sensor_info = '%d',
			ip_module = '%d',
			threshold = '%d',
			sensor_history = '%d'
		WHERE uuid = '%s'
		`,
		in.Permission.GetName(), boolToInt(in.Permission.GetUser()), boolToInt(in.Permission.GetPermission()), boolToInt(in.Permission.GetSettopCreate()),
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
		SELECT uuid, name, user, permission, settop_create, sensor_info, ip_module, threshold, sensor_history  
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
		var settop_create uint64
		var sensor_info uint64
		var ip_module uint64
		var threshold uint64
		var sensor_history uint64
		err := rows.Scan(&uuid, &name, &user, &permission, &settop_create, &sensor_info, &ip_module, &threshold, &sensor_history)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan permission row: %v", err)
		}
		if name == "master" {
			continue
		}
		permissionList := &pb.Permission{
			Uuid:          uuid,
			Name:          name,
			User:          intToBool(user),
			Permission:    intToBool(permission),
			SettopCreate:  intToBool(settop_create),
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
	var phone_number_aes256 []byte
	var query string
	query = fmt.Sprintf(`
		SELECT email, phone_number
		FROM user 
		WHERE name = '%s'
		`, in.GetName())
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&auth_email, &phone_number_aes256)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		phone_number_aes256, err = base64.StdEncoding.DecodeString(string(phone_number_aes256))
		if err != nil {
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		phone_number_aes256, err = AES256GSMDecrypt(aesSecretKey, phone_number_aes256)
		if err != nil {
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		phone_number := string(phone_number_aes256)
		if phone_number == in.GetPhoneNumber() {
			response.Email = auth_email
		} else {
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
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
		placeResponse, err := s.ReadPlaceList(ctx, &pb.ReadPlaceListRequest{})
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to retrieve place list: %v", err)
		}
		var settopList []*pb.Settop
		var sensorList []*pb.Sensor
		settopResponse, err := s.ReadSettopList(ctx, &pb.ReadSettopListRequest{})
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to retrieve settop list: %v", err)
		}
		for _, settop := range settopResponse.SettopList {
			sensorResponse, err := s.ReadSensorList(ctx, &pb.ReadSensorListRequest{
				SettopUuid: settop.GetUuid(),
			})
			if err != nil {
				log.Println(err)
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
			settop_.FwVersion = settop.GetFwVersion()
			settop_.RegisteredTime = settop.GetRegisteredTime()
			settopList = append(settopList, settop_)
			sensorList = append(sensorList, sensorResponse.GetSensorList()...)
		}
		response.PlaceList = placeResponse.GetPlaceList()
		response.SettopList = settopList
		response.SensorList = sensorList
		mainListMapping.AddMapping(in.GetRegistererUuid(), response)
		return response, nil
	}
}

func (s *server) StreamImage(ctx context.Context, req *pb.ImageRequest) (*pb.ImageChunk, error) {
	response := &pb.ImageChunk{}
	sensorSerial := req.SensorSerial
	date := req.Date
	location, err := time.LoadLocation("Asia/Seoul")
	requestTime, err := time.Parse("2006-01-02 15:04:05", date)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid date format: %v", err)
	}
	requestTime = requestTime.In(location).Add(-9 * time.Hour)
	start := requestTime.Add(-1 * time.Second)
	stop := requestTime.Add(1 * time.Second)
	startRFC3339 := start.Format(time.RFC3339)
	stopRFC3339 := stop.Format(time.RFC3339)

	query := fmt.Sprintf(`from(bucket: "%s")
    |> range(start: %s, stop: %s)
    |> filter(fn: (r) => r._field != "%s")
	|> filter(fn: (r) => r["_field"] == "image_data")
    `,
		Conf.InfluxDB.Bucket,
		startRFC3339,
		stopRFC3339,
		sensorSerial)
	results, err := _queryAPI.Query(context.Background(), query)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
	}
	var byteData string
	for results.Next() {
		if results.Record().Value() == nil {
			continue
		}
		byteData = results.Record().Value().(string)
	}
	if byteData == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
	}
	response.TempImage = byteData
	return response, nil
}

func (s *server) SubscribeFirebase(ctx context.Context, in *pb.SubscribeFirebaseRequest) (*pb.SubscribeFirebaseResponse, error) {
	log.Printf("SubscribeFirebase called ")
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
		return []byte(string(Conf.Jwt.SecretKeyAT)), nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
	}
	response := &pb.SubscribeFirebaseResponse{}
	requestData := make(map[string]string)
	requestData["token"] = in.GetToken()
	tokens := []string{requestData["token"]}
	var topics []string
	var group_master_uuid string
	if claims.Admin {
		query := fmt.Sprintf(`
			SELECT uuid 
			FROM group_ 
			WHERE name = 'master'
		`)
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&group_master_uuid)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}
		topics = append(topics, group_master_uuid)
	} else {
		topics = in.GetGroupUuid()
	}

	for _, topic := range topics {
		err = firebaseutil.SubscribeToTopic(tokens, topic, in.GetIsSubscribe())
		query := fmt.Sprintf(`
			SELECT COUNT(*) FROM firebase_token 
			WHERE token = '%s' AND group_uuid = '%s' AND email = '%s'`,
			in.GetToken(), topic, claims.Email)
		var count int
		err := db.QueryRow(query).Scan(&count)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		}
		if count == 0 {
			insertQuery := fmt.Sprintf(`
				INSERT INTO firebase_token (token, group_uuid, email)
				VALUES ('%s', '%s', '%s')`,
				in.GetToken(), topic, claims.Email)
			_, err := db.Exec(insertQuery)
			if err != nil {
				log.Println(err)
				return nil, status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			}
		}
	}
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
	}
	return response, nil
}

func (s *server) LogList(ctx context.Context, in *pb.LogListRequest) (*pb.LogListResponse, error) {
	log.Printf("LogList called")
	response := &pb.LogListResponse{}
	mainListResponse, ok := mainListMapping.GetMapping(in.GetRegistererUuid())
	if ok {
		fmt.Println("MainListResponse found")
	} else {
		mainListResponse, _ = s.MainList(ctx, &pb.MainListRequest{
			RegistererUuid: in.GetRegistererUuid(),
		})
	}
	query := ""
	if len(in.GetSensorSerial()) <= 0 {
		var sensorSerialClauses []string
		for _, sensor := range mainListResponse.SensorList {
			sensorSerialClauses = append(sensorSerialClauses, fmt.Sprintf("'%s'", sensor.Serial))
		}
		sensorSerialsCondition := strings.Join(sensorSerialClauses, ",")
		if sensorSerialsCondition == "" {
			return response, nil
		}
		query = fmt.Sprintf(`
		SELECT place, floor, room, sensor_name, type, registered_time  
		FROM log_ 
		WHERE sensor_serial IN (%s) 
		ORDER by id desc 
		LIMIT %d, %d 
	`, sensorSerialsCondition, in.GetCursor(), in.GetCount())
	} else {
		query = fmt.Sprintf(`
		SELECT place, floor, room, sensor_name, type, registered_time 
		FROM log_ 
		WHERE sensor_serial = '%s'
		ORDER by id desc 
		LIMIT %d, %d 
	`, in.GetSensorSerial(), in.GetCursor(), in.GetCount())
	}
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var place string
		var floor string
		var room string
		var sensor_name string
		var type_ uint64
		var registered_time string
		err := rows.Scan(&place, &floor, &room, &sensor_name, &type_, &registered_time)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to scan log row: %v", err)
		}
		logEntry := &pb.LogItem{
			Place:          place,
			Floor:          floor,
			Room:           room,
			SensorName:     sensor_name,
			Type:           pb.TypeStatus(type_),
			RegisteredTime: registered_time,
		}
		response.Log = append(response.Log, logEntry)
	}
	return response, nil
}

func (s *server) CreateRegistererGroup(ctx context.Context, in *pb.CreateRegistererGroupRequest) (*pb.CreateRegistererGroupResponse, error) {
	log.Printf("Received CreateRegistererGroup")
	response := &pb.CreateRegistererGroupResponse{}
	groupUUID := in.GetGroupUuid()
	registererUUIDs := in.GetRegistererUuid()
	var registerer_uuid sql.NullString
	query := fmt.Sprintf(`
		SELECT registerer_uuid 
		FROM group_gateway 
		WHERE group_uuid = '%s'
		`, in.GetGroupUuid())
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&registerer_uuid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		if getNullStringValidValue(registerer_uuid) == "" {
			continue
		}
		for _, registererUUID := range registererUUIDs {
			if registererUUID == getNullStringValidValue(registerer_uuid) {
				log.Println(err)
				err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
				return nil, err
			}
		}
	}
	for _, registererUUID := range registererUUIDs {
		query := fmt.Sprintf(`
            INSERT INTO group_gateway (group_uuid, registerer_uuid)
            VALUES ('%s', '%s')`,
			groupUUID, registererUUID)
		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer mainListMapping.RemoveMapping(registererUUID)
		defer sqlAddRegisterer.Close()
	}
	return response, nil
}

func (s *server) DeleteRegistererGroup(ctx context.Context, in *pb.DeleteRegistererGroupRequest) (*pb.DeleteRegistererGroupResponse, error) {
	log.Printf("Received DeleteRegistererGroup")
	query := fmt.Sprintf(`
		DELETE FROM group_gateway 
		WHERE group_uuid = '%s' AND registerer_uuid = '%s' 
		`,
		in.GetGroupUuid(), in.GetRegistererUuid())
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
	_, err = jwt.ParseWithClaims(token, claims, func(token *jwt.Token) (interface{}, error) {
		return []byte(string(Conf.Jwt.SecretKeyAT)), nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
	}
	var firebase_token string
	query = fmt.Sprintf(`
	SELECT token 
	FROM firebase_token 
	WHERE email = '%s' AND group_uuid = '%s' 
	`, claims.Email, in.GetGroupUuid())
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&firebase_token)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	if firebase_token != "" {
		requestData := make(map[string]string)
		requestData["token"] = firebase_token
		tokens := []string{requestData["token"]}
		err = firebaseutil.SubscribeToTopic(tokens, in.GetGroupUuid(), false)
		if err != nil {
			return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		}
		query = fmt.Sprintf(`
		DELETE FROM firebase_token 
		WHERE email = '%s' AND group_uuid = '%s' 
		`,
			claims.Email, in.GetGroupUuid())
		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Not Found Data: %v", err)
			return nil, err
		}
		defer sqlAddRegisterer.Close()
	}
	defer mainListMapping.RemoveMapping(in.GetGroupUuid())
	var group_uuid string
	query = fmt.Sprintf(`
			SELECT group_uuid 
			FROM group_gateway 
			WHERE registerer_uuid = '%s'
		`, in.GetRegistererUuid())
	rows1, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows1.Close()
	for rows1.Next() {
		err := rows1.Scan(&group_uuid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	if len(group_uuid) == 0 {
		var group_default_uuid string
		query := fmt.Sprintf(`
		SELECT uuid 
		FROM group_ 
		WHERE name = 'default'
	`)
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&group_default_uuid)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}
		query = fmt.Sprintf(`
			INSERT INTO group_gateway (group_uuid, registerer_uuid)
			VALUES ('%s', '%s')`,
			group_default_uuid, in.GetRegistererUuid())
		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer sqlAddRegisterer.Close()
	}
	return &pb.DeleteRegistererGroupResponse{}, nil
}

func (s *server) ReadRegistererGroupList(ctx context.Context, in *pb.ReadRegistererGroupListRequest) (*pb.ReadRegistererGroupListResponse, error) {
	log.Printf("Received GetGroupList: success")
	response := &pb.ReadRegistererGroupListResponse{}
	var registerer_uuid sql.NullString
	var uuid_ string
	var auth_email string
	var company_name string
	var company_number string
	var status_ pb.RegistererStatus
	var is_alarm uint64
	var permission_uuid string
	var name string
	var group_name string
	query := fmt.Sprintf(`
	SELECT name 
	FROM group_ 
	WHERE uuid = '%s'
	`, in.GetGroupUuid())
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&group_name)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	response.Name = group_name
	query = fmt.Sprintf(`
		SELECT registerer_uuid 
		FROM group_gateway 
		WHERE group_uuid = '%s'
		`, in.GetGroupUuid())
	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&registerer_uuid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		if getNullStringValidValue(registerer_uuid) == "" {
			continue
		}
		query = fmt.Sprintf(`
			SELECT uuid, auth_email, company_name, company_number, status, is_alarm, permission_uuid, name  
			FROM registerer 
			WHERE uuid = '%s'
			`,
			getNullStringValidValue(registerer_uuid))
		registerer_rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to fetch registerer information: %v", err)
		}
		registererList := &pb.Registerer{}
		for registerer_rows.Next() {
			err := registerer_rows.Scan(&uuid_, &auth_email, &company_name, &company_number, &status_, &is_alarm, &permission_uuid, &name)
			if err != nil {
				log.Println(err)
				return nil, status.Errorf(codes.Internal, "Failed to scan permission rows: %v", err)
			}
			registererList.Uuid = uuid_
			registererList.AuthEmail = auth_email
			registererList.CompanyName = company_name
			registererList.CompanyNumber = company_number
			registererList.Status = status_
			registererList.IsAlarm = intToBool(is_alarm)
			registererList.PermissionUuid = permission_uuid
			registererList.Name = name
		}
		response.RegistererList = append(response.RegistererList, registererList)
	}
	return response, nil
}

func (s *server) CreateSettopGroup(ctx context.Context, in *pb.CreateSettopGroupRequest) (*pb.CreateSettopGroupResponse, error) {
	log.Printf("Received CreateSettopGroup")
	response := &pb.CreateSettopGroupResponse{}
	settopUUID := in.GetSettopUuid()
	groupUUIDs := in.GetGroupUuid()
	var group_uuid string
	query := fmt.Sprintf(`
		SELECT group_uuid 
		FROM group_gateway 
		WHERE settop_uuid = '%s'
		`, in.GetSettopUuid())
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&group_uuid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		for _, groupUUID := range groupUUIDs {
			if groupUUID == group_uuid {
				log.Println(err)
				err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
				return nil, err
			}
		}
	}
	for _, groupUUID := range groupUUIDs {
		query := fmt.Sprintf(`
            INSERT INTO group_gateway (settop_uuid, group_uuid)
            VALUES ('%s', '%s')`,
			settopUUID, groupUUID)

		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer sqlAddRegisterer.Close()
	}
	return response, nil
}

func (s *server) DeleteSettopGroup(ctx context.Context, in *pb.DeleteSettopGroupRequest) (*pb.DeleteSettopGroupResponse, error) {
	log.Printf("Received DeleteRegistererGroup")
	query := fmt.Sprintf(`
		DELETE FROM group_gateway 
		WHERE group_uuid = '%s' AND settop_uuid = '%s' 
		`,
		in.GetGroupUuid(), in.GetSettopUuid())

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
	var group_uuid string
	query = fmt.Sprintf(`
			SELECT group_uuid 
			FROM group_gateway 
			WHERE settop_uuid = '%s'
		`, in.GetSettopUuid())
	rows1, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows1.Close()
	for rows1.Next() {
		err := rows1.Scan(&group_uuid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
	}
	if len(group_uuid) == 0 {
		var group_default_uuid string
		query := fmt.Sprintf(`
		SELECT uuid 
		FROM group_ 
		WHERE name = 'default'
	`)
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&group_default_uuid)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}
		query = fmt.Sprintf(`
			INSERT INTO group_gateway (group_uuid, settop_uuid)
			VALUES ('%s', '%s')`,
			group_default_uuid, in.GetSettopUuid())
		sqlAddRegisterer, err := db.Query(query)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			return nil, err
		}
		defer sqlAddRegisterer.Close()
	}
	return &pb.DeleteSettopGroupResponse{}, nil
}

func (s *server) ReadSettopGroupList(ctx context.Context, in *pb.ReadSettopGroupListRequest) (*pb.ReadSettopGroupListResponse, error) {
	log.Printf("Received GetSettopGroupList: success")
	response := &pb.ReadSettopGroupListResponse{}
	var group_uuid string
	var uuid_ string
	var name string
	var place_uuid string
	var serial string
	var address string
	query := fmt.Sprintf(`
		SELECT place_uuid, serial
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
		err := rows.Scan(&place_uuid, &serial)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}
	}
	query = fmt.Sprintf(`
		SELECT address 
		FROM place 
		WHERE uuid = '%s'
		`,
		place_uuid)
	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&address)
		if err != nil {
			log.Println(err)
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}
	}
	response.Serial = serial
	response.Address = address
	query = fmt.Sprintf(`
		SELECT group_uuid 
		FROM group_gateway 
		WHERE settop_uuid = '%s'
		`, in.GetSettopUuid())
	rows, err = db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&group_uuid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		query = fmt.Sprintf(`
			SELECT uuid, name  
			FROM group_ 
			WHERE uuid = '%s'
			`,
			group_uuid)
		registerer_rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.Internal, "Failed to fetch settop information: %v", err)
		}
		groupList := &pb.Group{}
		for registerer_rows.Next() {
			err := registerer_rows.Scan(&uuid_, &name)
			if err != nil {
				log.Println(err)
				return nil, status.Errorf(codes.Internal, "Failed to scan permission rows: %v", err)
			}
			groupList.Uuid = uuid_
			groupList.Name = name
		}
		response.GroupList = append(response.GroupList, groupList)
	}
	return response, nil
}

func roundToDecimalPlaces(value float32, decimalPlaces int) float32 {
	shift := math.Pow(10, float64(decimalPlaces))
	return float32(math.Round(float64(value)*shift) / shift)
}

func (s *server) MainGroupList(ctx context.Context, in *pb.MainGroupListRequest) (*pb.MainGroupListResponse, error) {
	log.Printf("Received MainGroupList")
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
		return []byte(string(Conf.Jwt.SecretKeyAT)), nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
	}
	response := &pb.MainGroupListResponse{
		Groups: make(map[string]*pb.MainGroup),
	}
	var registerer_uuid sql.NullString
	var uuid_ string
	var auth_email string
	var company_name string
	var company_number string
	var status_ pb.RegistererStatus
	var is_alarm uint64
	var permission_uuid string
	var name string
	var group_name string
	var group_uuid string
	query := fmt.Sprintf(`
		SELECT uuid, name 
		FROM group_ 
		`)
	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		err := rows.Scan(&group_uuid, &group_name)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		if group_name == "master" {
			continue
		}
		query = fmt.Sprintf(`
			SELECT registerer_uuid 
			FROM group_gateway 
			WHERE group_uuid = '%s'
		`, group_uuid)
		rows1, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer rows1.Close()
		mainGroup := &pb.MainGroup{}
		mainGroup.Name = group_name
		for rows1.Next() {
			err := rows1.Scan(&registerer_uuid)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			if getNullStringValidValue(registerer_uuid) == "" {
				continue
			}
			query = fmt.Sprintf(`
				SELECT uuid, auth_email, company_name, company_number, status, is_alarm, permission_uuid, name  
				FROM registerer 
				WHERE uuid = '%s'
				`,
				getNullStringValidValue(registerer_uuid))
			registerer_rows, err := db.Query(query)
			if err != nil {
				log.Println(err)
				return nil, status.Errorf(codes.Internal, "Failed to fetch registerer information: %v", err)
			}
			registererList := &pb.Registerer{}
			for registerer_rows.Next() {
				err := registerer_rows.Scan(&uuid_, &auth_email, &company_name, &company_number, &status_, &is_alarm, &permission_uuid, &name)
				if err != nil {
					log.Println(err)
					return nil, status.Errorf(codes.Internal, "Failed to scan permission rows: %v", err)
				}
				registererList.Uuid = uuid_
				registererList.AuthEmail = auth_email
				registererList.CompanyName = company_name
				registererList.CompanyNumber = company_number
				registererList.Status = status_
				registererList.IsAlarm = intToBool(is_alarm)
				registererList.PermissionUuid = permission_uuid
				registererList.Name = name
			}
			mainGroup.RegistererList = append(mainGroup.RegistererList, registererList)
		}
		response.Groups[group_uuid] = mainGroup
	}
	return response, nil
}

func (s *server) MainSettopList(ctx context.Context, in *pb.MainSettopListRequest) (*pb.MainSettopListResponse, error) {
	log.Printf("Received MainSettopListResponse")
	response := &pb.MainSettopListResponse{
		Settops: make(map[string]*pb.MainSettop),
	}
	var settop_uuid string
	var group_uuid sql.NullString
	var group_settop_uuid sql.NullString
	var uuid_ string
	var name string
	var place_uuid string
	var serial string
	var address string
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
		return []byte(string(Conf.Jwt.SecretKeyAT)), nil
	})
	if err != nil {
		return nil, status.Errorf(codes.Unauthenticated, "Invalid authentication token")
	}
	if claims.Admin {
		query := fmt.Sprintf(`
			SELECT uuid, place_uuid, serial
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
			err := rows.Scan(&settop_uuid, &place_uuid, &serial)
			if err != nil {
				log.Println(err)
				err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
				return nil, err
			}
			query = fmt.Sprintf(`
				SELECT address 
				FROM place 
				WHERE uuid = '%s'
				`,
				place_uuid)
			rows1, err := db.Query(query)
			if err != nil {
				log.Println(err)
				err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
				return nil, err
			}
			defer rows1.Close()
			for rows1.Next() {
				err := rows1.Scan(&address)
				if err != nil {
					log.Println(err)
					err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
					return nil, err
				}
			}
			mainSettop := &pb.MainSettop{}
			mainSettop.Serial = serial
			mainSettop.Address = address
			address = ""
			query = fmt.Sprintf(`
				SELECT group_uuid 
				FROM group_gateway 
				WHERE settop_uuid = '%s'
			`, settop_uuid)

			rows2, err := db.Query(query)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			defer rows2.Close()

			for rows2.Next() {
				err := rows2.Scan(&group_uuid)
				if err != nil {
					log.Println(err)
					return nil, err
				}
				query = fmt.Sprintf(`
					SELECT uuid, name  
					FROM group_ 
					WHERE uuid = '%s'
					`,
					getNullStringValidValue(group_uuid))
				registerer_rows, err := db.Query(query)
				if err != nil {
					log.Println(err)
					return nil, status.Errorf(codes.Internal, "Failed to fetch settop information: %v", err)
				}
				groupList := &pb.Group{}
				for registerer_rows.Next() {
					err := registerer_rows.Scan(&uuid_, &name)
					if err != nil {
						log.Println(err)
						return nil, status.Errorf(codes.Internal, "Failed to scan permission rows: %v", err)
					}
					groupList.Uuid = uuid_
					groupList.Name = name
				}
				mainSettop.GroupList = append(mainSettop.GroupList, groupList)
			}
			response.Settops[settop_uuid] = mainSettop
		}
	} else {
		readRegistererResponse, _ := s.ReadRegisterer(ctx, &pb.ReadRegistererRequest{
			Name: "check",
		})
		for _, group_uuid_ := range readRegistererResponse.GetRegistererInfo().GetGroupUuid() {
			query := fmt.Sprintf(`
				SELECT settop_uuid  
				FROM group_gateway 
				WHERE group_uuid = '%s'
				`, group_uuid_)
			rows, err := db.Query(query)
			if err != nil {
				log.Println(err)
				err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
				return nil, err
			}
			defer rows.Close()
			for rows.Next() {
				err := rows.Scan(&group_settop_uuid)
				if err != nil {
					log.Println(err)
					err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
					return nil, err
				}
				query := fmt.Sprintf(`
					SELECT uuid, place_uuid, serial
					FROM settop 
					WHERE uuid = '%s' 
					`, getNullStringValidValue(group_settop_uuid))
				rows, err := db.Query(query)
				if err != nil {
					log.Println(err)
					err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
					return nil, err
				}
				defer rows.Close()
				for rows.Next() {
					err := rows.Scan(&settop_uuid, &place_uuid, &serial)
					if err != nil {
						log.Println(err)
						err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
						return nil, err
					}
					query = fmt.Sprintf(`
						SELECT address 
						FROM place 
						WHERE uuid = '%s'
						`,
						place_uuid)
					rows1, err := db.Query(query)
					if err != nil {
						log.Println(err)
						err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
						return nil, err
					}
					defer rows1.Close()
					for rows1.Next() {
						err := rows1.Scan(&address)
						if err != nil {
							log.Println(err)
							err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
							return nil, err
						}
					}
					mainSettop := &pb.MainSettop{}
					mainSettop.Serial = serial
					mainSettop.Address = address
					address = ""
					query = fmt.Sprintf(`
						SELECT group_uuid 
						FROM group_gateway 
						WHERE settop_uuid = '%s'
					`, settop_uuid)
					rows2, err := db.Query(query)
					if err != nil {
						log.Println(err)
						return nil, err
					}
					defer rows2.Close()
					for rows2.Next() {
						err := rows2.Scan(&group_uuid)
						if err != nil {
							log.Println(err)
							return nil, err
						}
						query = fmt.Sprintf(`
							SELECT uuid, name  
							FROM group_ 
							WHERE uuid = '%s'
							`,
							getNullStringValidValue(group_uuid))
						registerer_rows, err := db.Query(query)
						if err != nil {
							log.Println(err)
							return nil, status.Errorf(codes.Internal, "Failed to fetch settop information: %v", err)
						}
						groupList := &pb.Group{}
						for registerer_rows.Next() {
							err := registerer_rows.Scan(&uuid_, &name)
							if err != nil {
								log.Println(err)
								return nil, status.Errorf(codes.Internal, "Failed to scan permission rows: %v", err)
							}
							groupList.Uuid = uuid_
							groupList.Name = name
						}
						mainSettop.GroupList = append(mainSettop.GroupList, groupList)
					}
					response.Settops[settop_uuid] = mainSettop
				}
			}
		}
	}
	return response, nil
}

func (s *server) ReadFirebaseTopicList(ctx context.Context, in *pb.ReadFirebaseTopicListRequest) (*pb.ReadFirebaseTopicListResponse, error) {
	log.Printf("Received ReadFirebaseTopicListResponse")
	response := &pb.ReadFirebaseTopicListResponse{}
	registererinfo, err := s.ReadRegisterer(ctx, &pb.ReadRegistererRequest{
		Name: "check",
	})
	if err != nil {
		log.Println(err)
	}
	var group_uuid string
	var uuid string
	var name string
	query := fmt.Sprintf(`
		SELECT group_uuid 
		FROM group_gateway 
		WHERE registerer_uuid = '%s'
	`, registererinfo.GetRegistererInfo().GetUuid())
	rows1, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows1.Close()
	for rows1.Next() {
		err := rows1.Scan(&group_uuid)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		query = fmt.Sprintf(`
			SELECT uuid, name 
			FROM group_ 
			WHERE uuid = '%s'
			`, group_uuid)
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, err
		}
		defer rows.Close()
		for rows.Next() {
			err := rows.Scan(&uuid, &name)
			if err != nil {
				log.Println(err)
				return nil, err
			}
			groupList := &pb.Group{}
			groupList.Uuid = uuid
			groupList.Name = name
			response.GroupList = append(response.GroupList, groupList)
		}
	}
	return response, nil
}
