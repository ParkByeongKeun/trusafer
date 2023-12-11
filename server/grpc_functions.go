package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"path"
	"time"

	pb "github.com/rudyryu/bacs-maincontrol/proto"
)

type server struct {
	pb.UnimplementedMainControlServer
}

func (s *server) AddRegisterer(ctx context.Context, in *pb.AddRegistererRequest) (*pb.AddRegistererResponse, error) {
	log.Printf("Received AddRegisterer: %d, %s, %s, %s, %s, %s, %s",
		in.Registerer.GetBarnId(), in.Registerer.GetName(), in.Registerer.GetPlateNumber(),
		in.Registerer.GetPhoneNumber(), in.Registerer.GetRegisterTime(),
		in.Registerer.GetStatus(), in.Registerer.GetNote())

	query := fmt.Sprintf(`
		INSERT INTO registerer SET
			barn_id = %d,
			name = '%s',
			plate_number = '%s',
			phone_number = '%s',
			registered_time = '%s', 
			status = %d,
			note = '%s'
		`,
		in.Registerer.GetBarnId(), in.Registerer.GetName(), in.Registerer.GetPlateNumber(),
		getOnlyNumbers(in.Registerer.GetPhoneNumber()), time.Now().Format("2006-01-02 15:04:05"),
		in.Registerer.GetStatus(), in.Registerer.GetNote())

	sqlAddRegisterer, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer sqlAddRegisterer.Close()

	return &pb.AddRegistererResponse{}, nil
}

func (s *server) UpdateRegisterer(ctx context.Context, in *pb.UpdateRegistererRequest) (*pb.UpdateRegistererResponse, error) {
	log.Printf("Received UpdateRegisterer: %s", in.Registerer.GetName())
	query := fmt.Sprintf(`
		UPDATE registerer SET
			barn_id = %d,
			name = '%s',
			plate_number = '%s',
			phone_number = '%s',
			registered_time = '%s', 
			status = %d,
			note = '%s'
		WHERE id = %d
		`,
		in.Registerer.GetBarnId(), in.Registerer.GetName(), in.Registerer.GetPlateNumber(),
		getOnlyNumbers(in.Registerer.GetPhoneNumber()), in.Registerer.GetRegisterTime(),
		in.Registerer.GetStatus(), in.Registerer.GetNote(), in.Registerer.GetId())

	sqlUpdateRegisterer, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	affectedCount, err := sqlUpdateRegisterer.RowsAffected()
	if err != nil {
		log.Println("affected count error after update query: ", err)
		return nil, err
	}
	log.Println("update users complete: ", affectedCount)

	return &pb.UpdateRegistererResponse{}, nil
}

func (s *server) DeleteRegisterer(ctx context.Context, in *pb.DeleteRegistererRequest) (*pb.DeleteRegistererResponse, error) {
	log.Printf("Received DeleteRegisterer: %d", in.GetRegistererId())

	query := fmt.Sprintf(`
		DELETE FROM registerer
		WHERE id = %d
		`,
		in.GetRegistererId())

	sqlDeleteRegisterer, err := db.Exec(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	nRow, err := sqlDeleteRegisterer.RowsAffected()
	if err != nil {
		log.Println(err)
		return nil, err
	}
	fmt.Println("delete count : ", nRow)
	return &pb.DeleteRegistererResponse{}, nil
}

func (s *server) GetRegisterer(ctx context.Context, in *pb.GetRegistererRequest) (*pb.GetRegistererResponse, error) {
	log.Printf("Received GetRegisterer: %d", in.RegistererId)
	response := &pb.GetRegistererResponse{}

	var id uint64
	var barn_id uint64
	var name string
	var plate_number string
	var phone_number string
	var register_time sql.NullString
	var status pb.RegistererStatus
	var note sql.NullString

	query := fmt.Sprintf(`
		SELECT id, barn_id, name, plate_number, phone_number, registered_time, status, note 
		FROM registerer 
		WHERE id = %d
		`,
		in.RegistererId)

	rows, err := db.Query(query)

	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &barn_id, &name, &plate_number, &phone_number, &register_time, &status, &note)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		registerer := &pb.Registerer{}
		registerer.Id = id
		registerer.BarnId = barn_id
		registerer.Name = name
		registerer.PlateNumber = plate_number
		registerer.PhoneNumber = phone_number
		registerer.RegisterTime = getNullStringValidValue(register_time)
		registerer.Status = status
		registerer.Note = getNullStringValidValue(note)

		response.Registerer = registerer
	}

	return response, nil
}

func (s *server) GetRegistererList(ctx context.Context, in *pb.GetRegistererListRequest) (*pb.GetRegistererListResponse, error) {
	log.Printf("Received GetBarnList: success")
	response := &pb.GetRegistererListResponse{}

	var id uint64
	var barn_id uint64
	var name string
	var plate_number string
	var phone_number string
	var register_time sql.NullString
	var status pb.RegistererStatus
	var note sql.NullString

	query := fmt.Sprintf(`
		SELECT id, barn_id, name, plate_number, phone_number, registered_time, status, note 
		FROM registerer 
		WHERE barn_id = %d
		`, in.GetBarnId())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &barn_id, &name, &plate_number, &phone_number, &register_time, &status, &note)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		registererList := &pb.Registerer{}
		registererList.Id = id
		registererList.BarnId = barn_id
		registererList.Name = name
		registererList.PlateNumber = plate_number
		registererList.PhoneNumber = phone_number
		registererList.RegisterTime = getNullStringValidValue(register_time)
		registererList.Status = status
		registererList.Note = getNullStringValidValue(note)

		response.RegistererList = append(response.RegistererList, registererList)
	}
	return response, nil
}

func (s *server) GetBarn(ctx context.Context, in *pb.GetBarnRequest) (*pb.GetBarnResponse, error) {
	log.Printf("Received GetBarn: %d", in.GetBarnId())
	response := &pb.GetBarnResponse{}

	var id uint64
	var name sql.NullString
	var address sql.NullString
	var latitude float64
	var longitude float64
	var phone_number sql.NullString
	var register_time sql.NullString
	var owner_name sql.NullString
	var owner_phone_number sql.NullString
	var status pb.BarnStatus
	var note sql.NullString
	var controller_serial sql.NullString
	var controller_version sql.NullString

	query := fmt.Sprintf(`
		SELECT id, name, address, latitude, longitude, phone_number, registered_time, 
			owner_name, owner_phone_number, status, note, 
			controller_serial, controller_version 
		FROM barn 
		where id = %d
		`,
		in.BarnId)

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &name, &address, &latitude, &longitude, &phone_number, &register_time,
			&owner_name, &owner_phone_number, &status, &note,
			&controller_serial, &controller_version)

		if err != nil {
			log.Println(err)
			return nil, err
		}

		barn := &pb.Barn{}
		barn.Id = id
		barn.Name = getNullStringValidValue(name)
		barn.Address = getNullStringValidValue(address)
		barn.Latitude = latitude
		barn.Longitude = longitude
		barn.PhoneNumber = getNullStringValidValue(phone_number)
		barn.RegisterTime = getNullStringValidValue(register_time)
		barn.OwnerName = getNullStringValidValue(owner_name)
		barn.OwnerPhoneNumber = getNullStringValidValue(owner_phone_number)
		barn.Status = status
		barn.Note = getNullStringValidValue(note)
		barn.ControllerSerial = getNullStringValidValue(controller_serial)
		barn.ControllerVersion = getNullStringValidValue(controller_version)

		response.Barn = barn
	}

	return response, nil
}

func (s *server) GetBarnList(ctx context.Context, in *pb.GetBarnListRequest) (*pb.GetBarnListResponse, error) {
	log.Printf("Received GetBarnList: success")
	response := &pb.GetBarnListResponse{}

	var id uint64
	var name sql.NullString
	var address sql.NullString
	var latitude float64
	var longitude float64
	var phone_number sql.NullString
	var register_time sql.NullString
	var owner_name sql.NullString
	var owner_phone_number sql.NullString
	var status pb.BarnStatus
	var note sql.NullString
	var controller_serial sql.NullString
	var controller_version sql.NullString

	query := fmt.Sprintf(`
		SELECT id, name, address, latitude, longitude, phone_number, registered_time, 
			owner_name, owner_phone_number, status, note, controller_serial, controller_version 
		FROM barn
	`)

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &name, &address, &latitude, &longitude, &phone_number, &register_time,
			&owner_name, &owner_phone_number, &status, &note, &controller_serial, &controller_version)

		if err != nil {
			log.Println(err)
			return nil, err
		}

		barn := &pb.Barn{}
		barn.Id = id
		barn.Name = getNullStringValidValue(name)
		barn.Address = getNullStringValidValue(address)
		barn.Latitude = latitude
		barn.Longitude = longitude
		barn.Address = getNullStringValidValue(address)
		barn.PhoneNumber = getNullStringValidValue(phone_number)
		barn.RegisterTime = getNullStringValidValue(register_time)
		barn.OwnerName = getNullStringValidValue(owner_name)
		barn.OwnerPhoneNumber = getNullStringValidValue(owner_phone_number)
		barn.Status = status
		barn.Note = getNullStringValidValue(note)
		barn.ControllerSerial = getNullStringValidValue(controller_serial)
		barn.ControllerVersion = getNullStringValidValue(controller_version)

		response.BarnList = append(response.BarnList, barn)
	}

	return response, nil
}

func (s *server) GetAccessLog(ctx context.Context, in *pb.GetAccessLogRequest) (*pb.GetAccessLogResponse, error) {
	log.Printf("Received GetAccessLog: %d", in.GetBarnId())
	response := &pb.GetAccessLogResponse{}

	var id uint64
	var barn_id uint64
	var plate_number string
	var access_time string
	var is_opened bool
	var note sql.NullString

	// 나중에
	// 갯수 제한 넣기(일단 1000개 LIMIT)
	// 앱에서 스크롤내리면 더 가져오기
	query := fmt.Sprintf(`
		SELECT id, barn_id, plate_number, time, is_opened, log
		FROM history
		WHERE barn_id = %d
		ORDER BY time DESC
		LIMIT 1000;
		`,
		in.GetBarnId())

	rows, err := db.Query(query)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		err := rows.Scan(&id, &barn_id, &plate_number, &access_time, &is_opened, &note)
		if err != nil {
			log.Println(err)
			return nil, err
		}

		history := &pb.AccessLog{}
		history.BarnId = barn_id
		history.PlateNumber = plate_number
		history.AccessTime = access_time
		history.IsOpened = is_opened
		history.Note = getNullStringValidValue(note)

		response.AccessLog = append(response.AccessLog, history)
	}

	return response, nil
}

func (s *server) Access(ctx context.Context, in *pb.AccessRequest) (*pb.AccessResponse, error) {
	log.Printf("Received Access: %s %s", in.GetControllerSerial(), in.GetPlateNumber())
	/*
		1. controller_serial을 통해 barn_id 획득
		2. barn_id, plate_number를 통해 등록 명부를 조회하고 해당 명부의 status 획득
		3. history table에 개폐 결과 INSERT
		4. return AccessResponse
	*/

	var err error

	currentTime := time.Now()

	var barn_id int = -1
	var status int = -1
	// 1. controller_serial을 통해 barn_id 획득
	var query string
	query = fmt.Sprintf(`
		SELECT id
		FROM barn
		WHERE controller_serial = '%s'
		`,
		in.GetControllerSerial())

	err = db.QueryRow(query).Scan(&barn_id)
	if err != nil {
		log.Println(err)
	}

	// 2. barn_id, plate_number를 통해 등록 명부를 조회하고 해당 명부의 status 획득
	query = fmt.Sprintf(`
		SELECT status
		FROM registerer
		WHERE barn_id = %d AND plate_number = '%s'
		`,
		barn_id, in.GetPlateNumber())

	err = db.QueryRow(query).Scan(&status)
	if err != nil {
		log.Println(err)
	}

	var open bool
	var is_opened string
	var access_log string

	switch status {
	case int(pb.RegistererStatus_REGISTERER_STATUS_REGISTERED):
		open = true
		is_opened = "TRUE"
		access_log = fmt.Sprintf("등록된 번호판입니다.")
	case int(pb.RegistererStatus_REGISTERER_STATUS_BLOCKED):
		open = false
		is_opened = "FALSE"
		access_log = fmt.Sprintf("차단된 번호판입니다.")
	default:
		open = false
		is_opened = "FALSE"
		access_log = fmt.Sprintf("등록되지 않은 번호판입니다.")
	}

	// saveImagePath
	// ex) ksf00001/2021-01-01_12:00:00_(56부1414).jpeg

	log.Println(saveImageDir)
	imageFile := currentTime.Format("2006-01-02_15:04:05") + "_" + "(" + in.PlateNumber + ")"
	saveImagePath := path.Join(saveImageDir, in.GetControllerSerial(), imageFile+".jpg")
	saveImagePathDB := path.Join(in.GetControllerSerial(), imageFile+".jpg")
	saveImageSuccess, err := saveJpegBytesImage(in.Image, saveImagePath)
	if err != nil {
		log.Println("saveJpegBytesImage error")
		log.Println(err)
	}

	if saveImageSuccess {
		log.Printf("이미지 저장 완료: %s", saveImagePath)
		query = fmt.Sprintf(`
			INSERT INTO history SET
				barn_id = %d,
				plate_number = '%s',
				time = '%s',
				is_opened = %s,
				log = '%s',
				image_path = '%s';
			`,
			barn_id, in.GetPlateNumber(), currentTime.Format("2006-01-02 15:04:05"), is_opened, access_log, saveImagePathDB)
	} else {
		log.Printf("이미지 저장 실패: %s", saveImagePath)
		query = fmt.Sprintf(`
			INSERT INTO history SET
				barn_id = %d,
				plate_number = '%s',
				time = '%s',
				is_opened = %s,
				log = '%s';
			`,
			barn_id, in.GetPlateNumber(), currentTime.Format("2006-01-02 15:04:05"), is_opened, access_log)
	}

	_, err = db.Exec(query)
	if err != nil {
		log.Println(err)
	}

	fmt.Println(barn_id, status, in.GetPlateNumber(), currentTime.Format("2006-01-02 15:04:05"), is_opened, access_log)
	return &pb.AccessResponse{Open: open}, err
}

func (s *server) OpenGateFromApp(ctx context.Context, in *pb.OpenGateFromAppRequest) (*pb.OpenGateFromAppResponse, error) {
	log.Printf("Received Open: %d", in.GetBarnId())
	// OpenGateFromAppRequest 할때 db history 테이블에 저장할 정보 필요
	// now_utc := time.Now().UTC()
	// sqlOpenGate, errOpenGateInsert := db.Query("INSERT INTO history(registerer_id, access_time) VALUES(?, ?)", in.RegistererId, now_utc)
	// if errOpenGateInsert != nil {
	// 	log.Println(errOpenGateInsert)
	// }
	// defer sqlOpenGate.Close()
	return &pb.OpenGateFromAppResponse{}, nil
}
