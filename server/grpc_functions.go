package main

import (
	"context"
	"fmt"
	"log"

	pb "github.com/ParkByeongKeun/trusafer-idl/maincontrol"
	"github.com/dgrijalva/jwt-go"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

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
	query := fmt.Sprintf(`
		DELETE FROM registerer
		WHERE uuid = '%s'
		`,
		in.GetRegistererUuid())

	sqlDeleteRegisterer, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}

	nRow, err := sqlDeleteRegisterer.RowsAffected()
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
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

	// Fetch registerer information
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

		// Fetch permission information
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

		// Build response for registerer information
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
		// If no registerer info found, create a new one with default permission
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

		// Build response for newly created registerer
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
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &authEmail, &companyName, &companyNumber, &status_, &isAlarm, &permissionUUID, &name)
		if err != nil {
			log.Println(err)
			// gRPC 오류를 생성하여 상태 코드 설정
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
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	response.Uuid = uuid.String()
	defer sqlAddPlace.Close()

	return response, nil
}

func (s *server) UpdatePlace(ctx context.Context, in *pb.UpdatePlaceRequest) (*pb.UpdatePlaceResponse, error) {
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
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	affectedCount, err := sqlUpdatePlace.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	log.Println("update place complete: ", affectedCount)

	return &pb.UpdatePlaceResponse{}, nil
}

func (s *server) DeletePlace(ctx context.Context, in *pb.DeletePlaceRequest) (*pb.DeletePlaceResponse, error) {
	log.Printf("Received DeletePlace: %s", in.GetPlaceUuid())

	query := fmt.Sprintf(`
		DELETE FROM place
		WHERE uuid = '%s'
		`,
		in.GetPlaceUuid())

	sqlDeletePlace, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeletePlace.RowsAffected()
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
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
		SELECT id, name, address, registerer_uuid, registered_time 
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
	log.Printf("Received GetPlaceList: success = %s", in.GetRegistererUuid())
	response := &pb.ReadPlaceListResponse{}

	var uuid string
	var name string
	var address string
	var registererUUID string
	var registeredTime string

	query := fmt.Sprintf(`
		SELECT uuid, name, address, registerer_uuid, registered_time 
		FROM place 
		WHERE registerer_uuid = '%s'
		`, in.GetRegistererUuid())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &name, &address, &registererUUID, &registeredTime)
		if err != nil {
			log.Println(err)
			// gRPC 오류를 생성하여 상태 코드 설정
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
	log.Printf("Received AddSettop: %s, %s, %s, %s, %d, %s",
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
			floor = '%d',
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
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	response.Uuid = uuid.String()
	defer sqlAddRegisterer.Close()
	return response, nil
}

func (s *server) UpdateSettop(ctx context.Context, in *pb.UpdateSettopRequest) (*pb.UpdateSettopResponse, error) {
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
			serial = '%s',
			room = '%s',
			floor = '%d',
			mac1 = '%s',
			mac2 = '%s',
			is_alive = '%d',
			latest_version = '%s',
			registered_time = '%s'
		WHERE uuid = '%s'
		`,
		in.Settop.GetSerial(),
		in.Settop.GetRoom(), in.Settop.GetFloor(), in.Settop.GetMac1(), in.Settop.GetMac2(),
		boolToInt(in.Settop.GetIsAlive()), in.Settop.GetLatestVersion(), in.Settop.GetRegisteredTime(), in.Settop.GetPlaceUuid())

	sqlUpdateSettop, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	affectedCount, err := sqlUpdateSettop.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	log.Println("update settop complete: ", affectedCount)

	return &pb.UpdateSettopResponse{}, nil
}

func (s *server) DeleteSettop(ctx context.Context, in *pb.DeleteSettopRequest) (*pb.DeleteSettopResponse, error) {
	log.Printf("Received DeleteSettop: %s", in.GetSettopUuid())

	query := fmt.Sprintf(`
		DELETE FROM settop
		WHERE uuid = '%s'
		`,
		in.GetSettopUuid())

	sqlDeleteSettop, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeleteSettop.RowsAffected()
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
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
	var floor uint64
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
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &place_uuid, &serial, &room, &floor, &mac1, &mac2, &is_alive, &latest_version, &registered_time)
		if err != nil {
			log.Println(err)
			// gRPC 오류를 생성하여 상태 코드 설정
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}

		settop := &pb.Settop{}
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
	var floor uint64
	var mac1 string
	var mac2 string
	var is_alive bool
	var latest_version string
	var registered_time string

	query := fmt.Sprintf(`
		SELECT uuid, place_uuid, serial, room, floor, mac1, mac2, is_alive, latest_version, registered_time  
		FROM settop 
		WHERE place_uuid = '%s'
		`, in.GetPlaceUuid())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &place_uuid, &serial, &room, &floor, &mac1, &mac2, &is_alive, &latest_version, &registered_time)
		if err != nil {
			log.Println(err)
			// gRPC 오류를 생성하여 상태 코드 설정
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}

		settopList := &pb.Settop{}
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

		response.SettopList = append(response.SettopList, settopList)
	}
	return response, nil
}

func (s *server) CreateSensor(ctx context.Context, in *pb.CreateSensorRequest) (*pb.CreateSensorResponse, error) {
	permission := s.getPermission(ctx)
	if !permission.SensorCreate {
		log.Println("err permission")
		return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
	}
	log.Printf("Received AddSensor: %s, %s, %d, %s, %s, %s, %s, %s, %s, %s",
		in.Sensor.GetUuid(), in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(),
		in.Sensor.GetThresholdTempWarning(), in.Sensor.GetThresholdTempDanger(), in.Sensor.GetLatestVersion(),
		in.Sensor.GetRegisteredTime())
	var uuid = uuid.New()
	response := &pb.CreateSensorResponse{}

	query := fmt.Sprintf(`
		INSERT INTO sensor SET
			uuid = '%s', 
			settop_uuid = '%s',
			status = '%d',
			serial = '%s',
			ip_address = '%s',
			location = '%s',
			threshold_temp_warning = '%s',
			threshold_temp_danger = '%s',
			latest_version = '%s',
			registered_time = '%s'
		`,
		uuid.String(), in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(), in.Sensor.GetThresholdTempWarning(),
		in.Sensor.GetThresholdTempDanger(), in.Sensor.GetLatestVersion(),
		in.Sensor.GetRegisteredTime())

	sqlAddSensor, err := db.Query(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	response.Uuid = uuid.String()
	defer sqlAddSensor.Close()

	return response, nil
}

func (s *server) UpdateSensor(ctx context.Context, in *pb.UpdateSensorRequest) (*pb.UpdateSensorResponse, error) {
	permission := s.getPermission(ctx)
	if !permission.SensorInfo {
		log.Println("err permission")
		return nil, status.Errorf(codes.PermissionDenied, "Err Permission")
	}
	var alarm_warning string
	var alarm_danger string

	if !permission.Threshold {
		query := fmt.Sprintf(`
        SELECT threshold_temp_warning, threshold_temp_danger
        FROM sensor
        WHERE uuid = '%s'
        LIMIT 1
    `, in.GetSensor().GetUuid())
		rows, err := db.Query(query)
		if err != nil {
			log.Println(err)
			return nil, status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		}
		defer rows.Close()

		for rows.Next() {
			err := rows.Scan(&alarm_warning, &alarm_danger)
			if err != nil {
				log.Println(err)
				return nil, err
			}
		}

		if alarm_warning != in.Sensor.ThresholdTempWarning || alarm_danger != in.Sensor.ThresholdTempDanger {
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
			threshold_temp_warning = '%s',
			threshold_temp_danger = '%s',
			latest_version = '%s',
			registered_time = '%s'
		WHERE uuid = '%s'
		`,
		in.Sensor.GetSettopUuid(), in.Sensor.GetStatus(),
		in.Sensor.GetSerial(), in.Sensor.GetIpAddress(), in.Sensor.GetLocation(), in.Sensor.GetThresholdTempWarning(),
		in.Sensor.GetThresholdTempDanger(), in.Sensor.GetLatestVersion(),
		in.Sensor.GetRegisteredTime(), in.Sensor.GetUuid())

	sqlUpdateSensor, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	affectedCount, err := sqlUpdateSensor.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
		return nil, err
	}
	log.Println("update sensor complete: ", affectedCount)

	return &pb.UpdateSensorResponse{}, nil
}

func (s *server) DeleteSensor(ctx context.Context, in *pb.DeleteSensorRequest) (*pb.DeleteSensorResponse, error) {
	log.Printf("Received DeleteSensor: %s", in.GetSensorUuid())

	query := fmt.Sprintf(`
		DELETE FROM sensor
		WHERE uuid = '%s'
		`,
		in.GetSensorUuid())

	sqlDeleteSensor, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	nRow, err := sqlDeleteSensor.RowsAffected()
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
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
	var threshold_temp_warning string
	var threshold_temp_danger string
	var latest_version string
	var registered_time string

	query := fmt.Sprintf(`
		SELECT uuid, settop_uuid, status, serial, ip_address, location, threshold_temp_warning, threshold_temp_danger, latest_version, registered_time  
		FROM sensor 
		WHERE uuid = '%s'
		`,
		in.GetSensorUuid())

	rows, err := db.Query(query)

	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &settop_uuid, &status_, &serial, &ip_address, &location, &threshold_temp_warning, &threshold_temp_danger, &latest_version, &registered_time)
		if err != nil {
			log.Println(err)
			// gRPC 오류를 생성하여 상태 코드 설정
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
		sensor.ThresholdTempWarning = threshold_temp_warning
		sensor.ThresholdTempDanger = threshold_temp_danger
		sensor.LatestVersion = latest_version
		sensor.RegisteredTime = registered_time
		response.Sensor = sensor
	}

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
	var threshold_temp_warning string
	var threshold_temp_danger string
	var latest_version string
	var registered_time string

	query := fmt.Sprintf(`
		SELECT uuid, settop_uuid, status, serial, ip_address, location, threshold_temp_warning, threshold_temp_danger, latest_version, registered_time  
		FROM sensor 
		WHERE settop_uuid = '%s'
		`, in.GetSettopUuid())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		// gRPC 오류를 생성하여 상태 코드 설정
		err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&uuid, &settop_uuid, &status_, &serial, &ip_address, &location, &threshold_temp_warning, &threshold_temp_danger, &latest_version, &registered_time)
		if err != nil {
			log.Println(err)
			// gRPC 오류를 생성하여 상태 코드 설정
			err = status.Errorf(codes.Internal, "Internal Server Error: %v", err)
			return nil, err
		}

		sensorList := &pb.Sensor{}
		sensorList.Uuid = uuid
		sensorList.SettopUuid = settop_uuid
		sensorList.Status = pb.SensorStatus(status_)
		sensorList.Serial = serial
		sensorList.IpAddress = ip_address
		sensorList.Location = location
		sensorList.ThresholdTempWarning = threshold_temp_warning
		sensorList.ThresholdTempDanger = threshold_temp_danger
		sensorList.LatestVersion = latest_version
		sensorList.RegisteredTime = registered_time

		response.SensorList = append(response.SensorList, sensorList)
	}
	return response, nil
}

func (s *server) CreateHistory(ctx context.Context, in *pb.CreateHistoryRequest) (*pb.CreateHistoryResponse, error) {
	log.Printf("Received AddHistory: %s, %s, %d, %d, %s",
		in.History.GetUuid(), in.History.GetSensorSerial(), in.History.GetMinTemp(), in.History.GetMaxTemp(), in.History.GetDate())
	var uuid = uuid.New()

	query := fmt.Sprintf(`
		INSERT INTO history SET
			uuid = '%s', 
			sensor_serial = '%s',
			min_temp = '%d',
			max_temp = '%d',
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
	var min_temp uint64
	var max_temp uint64
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
	var min_temp uint64
	var max_temp uint64
	var date string

	query := fmt.Sprintf(`
		SELECT uuid, sensor_serial, min_temp, max_temp, date  
		FROM history 
		WHERE sensor_serial = '%s'
		`, in.GetSensorSerial())

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

// func (s *server) CreatIpModule(ctx context.Context, in *pb.CreateIpModuleRequest) (*pb.CreateIpModuleResponse, error) {
// 	log.Printf("Received AddIpModule: %d, %d, %s, %s, %s",
// 		in.IpModule.GetId(), in.IpModule.GetSettopId(), in.IpModule.GetIpAddress(), in.IpModule.GetMacAddress(), in.IpModule.GetFirmwareVersion())

// 	query := fmt.Sprintf(`
// 		INSERT INTO ip_module SET
// 			settop_id = '%d',
// 			ip_address = '%s',
// 			mac_address = '%s',
// 			firmware_version = '%s'
// 		`,
// 		in.IpModule.GetSettopId(), in.IpModule.GetIpAddress(), in.IpModule.GetMacAddress(), in.IpModule.GetFirmwareVersion())

// 	sqlAddRegisterer, err := db.Query(query)
// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}
// 	defer sqlAddRegisterer.Close()

// 	return &pb.CreateIpModuleResponse{}, nil
// }

// func (s *server) UpdateIpModule(ctx context.Context, in *pb.UpdateIpModuleRequest) (*pb.UpdateIpModuleResponse, error) {
// 	log.Printf("Received UpdateIpModule: %d", in.IpModule.GetId())
// 	query := fmt.Sprintf(`
// 		UPDATE ip_module SET
// 			settop_id = '%d',
// 			ip_address = '%s',
// 			mac_address = '%s',
// 			firmware_version = '%s'
// 		WHERE id = %d
// 		`,
// 		in.IpModule.GetSettopId(), in.IpModule.GetIpAddress(), in.IpModule.GetMacAddress(), in.IpModule.GetFirmwareVersion(), in.IpModule.GetId())

// 	sqlUpdateRegisterer, err := db.Exec(query)
// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}
// 	affectedCount, err := sqlUpdateRegisterer.RowsAffected()
// 	if err != nil {
// 		log.Println("affected count error after update query: ", err)
// 		return nil, err
// 	}
// 	log.Println("update ip_module complete: ", affectedCount)

// 	return &pb.UpdateIpModuleResponse{}, nil
// }

// func (s *server) DeleteIpModule(ctx context.Context, in *pb.DeleteIpModuleRequest) (*pb.DeleteIpModuleResponse, error) {
// 	log.Printf("Received DeleteIpModule: %d", in.GetIpModuleId())

// 	query := fmt.Sprintf(`
// 		DELETE FROM ip_module
// 		WHERE id = %d
// 		`,
// 		in.GetIpModuleId())

// 	sqlDeleteRegisterer, err := db.Exec(query)
// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}
// 	nRow, err := sqlDeleteRegisterer.RowsAffected()
// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}
// 	fmt.Println("delete count : ", nRow)
// 	return &pb.DeleteIpModuleResponse{}, nil
// }

// func (s *server) ReadIpModule(ctx context.Context, in *pb.ReadIpModuleRequest) (*pb.ReadIpModuleResponse, error) {
// 	log.Printf("Received GetIpModule: %d", in.IpModuleId)
// 	response := &pb.ReadIpModuleResponse{}

// 	var id uint64
// 	var settop_id uint64
// 	var ip_address string
// 	var mac_address string
// 	var firmware_version string

// 	query := fmt.Sprintf(`
// 		SELECT id, settop_id, ip_address, mac_address, firmware_version
// 		FROM ip_module
// 		WHERE id = %d
// 		`,
// 		in.IpModuleId)

// 	rows, err := db.Query(query)

// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		err := rows.Scan(&id, &settop_id, &ip_address, &mac_address, &firmware_version)
// 		if err != nil {
// 			log.Println(err)
// 			return nil, err
// 		}

// 		ipmodule := &pb.IpModule{}
// 		ipmodule.Id = id
// 		ipmodule.SettopId = settop_id
// 		ipmodule.IpAddress = ip_address
// 		ipmodule.MacAddress = mac_address
// 		ipmodule.FirmwareVersion = firmware_version

// 		response.IpModule = ipmodule
// 	}

// 	return response, nil
// }

// func (s *server) ReadIpModuleList(ctx context.Context, in *pb.ReadIpModuleListRequest) (*pb.ReadIpModuleListResponse, error) {
// 	log.Printf("Received GetIpModuleList: success")
// 	response := &pb.ReadIpModuleListResponse{}

// 	var id uint64
// 	var settop_id uint64
// 	var ip_address string
// 	var mac_address string
// 	var firmware_version string

// 	query := fmt.Sprintf(`
// 		SELECT id, settop_id, ip_address, mac_address, firmware_version
// 		FROM ip_module
// 		WHERE sensor_id = %d
// 		`, in.GetSettopId())

// 	rows, err := db.Query(query)
// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	for rows.Next() {
// 		err := rows.Scan(&id, &settop_id, &ip_address, &mac_address, &firmware_version)
// 		if err != nil {
// 			log.Println(err)
// 			return nil, err
// 		}

// 		ipmoduleList := &pb.IpModule{}
// 		ipmoduleList.Id = id
// 		ipmoduleList.SettopId = settop_id
// 		ipmoduleList.IpAddress = ip_address
// 		ipmoduleList.MacAddress = mac_address
// 		ipmoduleList.FirmwareVersion = firmware_version

// 		response.IpModuleList = append(response.IpModuleList, ipmoduleList)
// 	}
// 	return response, nil
// }

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

	return response, nil
}

func (s *server) MainList(ctx context.Context, in *pb.MainListRequest) (*pb.MainListResponse, error) {
	log.Printf("MainList called")
	response := &pb.MainListResponse{}

	// Place List
	placeResponse, err := s.ReadPlaceList(ctx, &pb.ReadPlaceListRequest{
		RegistererUuid: in.GetRegistererUuid(),
	})
	if err != nil {
		log.Println(err)
		// You can customize the error response and status code based on your application's requirements.
		return nil, status.Errorf(codes.Internal, "Failed to retrieve place list: %v", err)
	}

	var settopList []*pb.Settop
	var sensorList []*pb.Sensor

	// Iterate through each place to get settops and sensors
	for _, place := range placeResponse.PlaceList {

		// Settop List for the current place
		settopResponse, err := s.ReadSettopList(ctx, &pb.ReadSettopListRequest{
			PlaceUuid: place.GetUuid(),
		})
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

			// Add settops to the list
			settopList = append(settopList, settop)

			// Add sensors to the list
			sensorList = append(sensorList, sensorResponse.GetSensorList()...)
		}
	}

	response.PlaceList = placeResponse.GetPlaceList()
	response.SettopList = settopList
	response.SensorList = sensorList
	return response, nil
}
