package main

import (
	"fmt"
	"log"

	"github.com/google/uuid"
)

func createDBTablesIfNotExist() error {

	log.Println("Create db tables if not exist")

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS place (
			uuid VARCHAR(36) NOT NULL,
			name varchar(50) DEFAULT NULL,
			address varchar(255) DEFAULT NULL,
			registerer_uuid VARCHAR(36) DEFAULT NULL,
			registered_time datetime DEFAULT NULL,
			PRIMARY KEY (uuid),
			KEY trusafer_id_IDX (uuid,name,address,registerer_uuid,registered_time) USING BTREE
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreatetrusafer, err := db.Query(query)
	if err != nil {
		log.Printf(" - create place table err: %s", err)
		return err
	}
	log.Println(" - place table OK")
	defer sqlCreatetrusafer.Close()

	// // create history
	// query = fmt.Sprintf(`
	// 	CREATE TABLE IF NOT EXISTS history (
	// 		id int(11) NOT NULL AUTO_INCREMENT,
	// 		min_temp FLOAT NOT NULL,
	// 		max_temp FLOAT NOT NULL,
	// 		date datetime NOT NULL,
	// 		PRIMARY KEY (id),
	// 		KEY fk_history_trusafer (id)
	// 	) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	// `)
	// sqlCreateHistory, err := db.Query(query)
	// if err != nil {
	// 	log.Printf(" - create history err: %s", err)
	// 	return err
	// }
	// log.Println(" - History table OK")
	// defer sqlCreateHistory.Close()

	// create registerer
	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS registerer (
			uuid VARCHAR(36) NOT NULL,
			auth_email varchar(255) UNIQUE NOT NULL,
			company_name varchar(20) NOT NULL,
			company_number varchar(20) NOT NULL,
			status smallint(5) unsigned DEFAULT 0,
			is_alarm int(11) NOT NULL,
			permission_uuid VARCHAR(36) DEFAULT NULL, 
			name varchar(20) NOT NULL, 
			PRIMARY KEY (uuid)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateRegisterer, err := db.Query(query)
	if err != nil {
		log.Printf(" - create registerer table err: %s", err)
		return err
	}
	log.Println(" - Registerer table OK")
	defer sqlCreateRegisterer.Close()

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS settop (
			uuid VARCHAR(36) NOT NULL,
			place_uuid VARCHAR(36) DEFAULT NULL,
			serial varchar(255) UNIQUE DEFAULT NULL,
			room varchar(255) DEFAULT NULL,
			floor varchar(255) DEFAULT NULL,
			mac1 varchar(255) DEFAULT NULL,
			mac2 varchar(255) DEFAULT NULL,
			is_alive int(11) DEFAULT NULL,
			fw_version varchar(255) DEFAULT NULL,
			registered_time datetime DEFAULT NULL,
			PRIMARY KEY (uuid)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateSettop, err := db.Query(query)
	if err != nil {
		log.Printf(" - create settop table err: %s", err)
		return err
	}
	log.Println(" - settop table OK")
	defer sqlCreateSettop.Close()

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS sensor (
			uuid VARCHAR(36) NOT NULL,
			settop_uuid VARCHAR(36) DEFAULT NULL,
			status int(11) DEFAULT NULL,
			serial varchar(255) UNIQUE DEFAULT NULL,
			ip_address varchar(255) DEFAULT NULL,
			location varchar(255) DEFAULT NULL,
			registered_time datetime DEFAULT NULL,
			mac varchar(255) DEFAULT NULL,
			name varchar(255) DEFAULT NULL,
			type varchar(36) DEFAULT NULL,
			PRIMARY KEY (uuid)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateSensor, err := db.Query(query)
	if err != nil {
		log.Printf(" - create sensor table err: %s", err)
		return err
	}
	log.Println(" - sensor table OK")
	defer sqlCreateSensor.Close()

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS threshold (
			sensor_uuid VARCHAR(36) NOT NULL,
			temp_warning1 varchar(255) DEFAULT NULL,
			temp_danger1 varchar(255) DEFAULT NULL,
			temp_warning2 varchar(255) DEFAULT NULL,
			temp_danger2 varchar(255) DEFAULT NULL,
			temp_warning3 varchar(255) DEFAULT NULL,
			temp_danger3 varchar(255) DEFAULT NULL,
			temp_warning4 varchar(255) DEFAULT NULL,
			temp_danger4 varchar(255) DEFAULT NULL,
			temp_warning5 varchar(255) DEFAULT NULL,
			temp_danger5 varchar(255) DEFAULT NULL,
			temp_warning6 varchar(255) DEFAULT NULL,
			temp_danger6 varchar(255) DEFAULT NULL,
			temp_warning7 varchar(255) DEFAULT NULL,
			temp_danger7 varchar(255) DEFAULT NULL,
			temp_warning8 varchar(255) DEFAULT NULL,
			temp_danger8 varchar(255) DEFAULT NULL,
			temp_warning9 varchar(255) DEFAULT NULL,
			temp_danger9 varchar(255) DEFAULT NULL,
			PRIMARY KEY (sensor_uuid)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateThreshold, err := db.Query(query)
	if err != nil {
		log.Printf(" - create Threshold table err: %s", err)
		return err
	}
	log.Println(" - Threshold table OK")
	defer sqlCreateThreshold.Close()

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS group_ (
			uuid VARCHAR(36) NOT NULL,
			name varchar(255) UNIQUE DEFAULT NULL,
			PRIMARY KEY (uuid)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateGroup, err := db.Query(query)
	if err != nil {
		log.Printf(" - create group table err: %s", err)
		return err
	}
	log.Println(" - group table OK")
	defer sqlCreateGroup.Close()
	initGroup()

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS group_gateway (
			group_uuid VARCHAR(36) NOT NULL,
			registerer_uuid VARCHAR(36) DEFAULT NULL,
			settop_uuid VARCHAR(36) DEFAULT NULL 
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateGroup, err = db.Query(query)
	if err != nil {
		log.Printf(" - create group_settop table err: %s", err)
		return err
	}
	log.Println(" - group table OK")
	defer sqlCreateGroup.Close()

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS log_ (
			id int(11) NOT NULL AUTO_INCREMENT,
			place VARCHAR(36) NOT NULL,
			floor VARCHAR(36) NOT NULL,
			room VARCHAR(36) NOT NULL,
			sensor_name VARCHAR(36) NOT NULL,
			sensor_serial VARCHAR(36) NOT NULL,
			type smallint(5) unsigned DEFAULT 0,
			registered_time datetime DEFAULT NULL,
			PRIMARY KEY (id)
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateLog, err := db.Query(query)
	if err != nil {
		log.Printf(" - create log table err: %s", err)
		return err
	}
	log.Println(" - log table OK")
	defer sqlCreateLog.Close()

	query = fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS permission (
		uuid VARCHAR(36) NOT NULL,
		name varchar(255) UNIQUE DEFAULT NULL,
		user int(11) NOT NULL,
		permission int(11) NOT NULL,
		settop_create int(11) NOT NULL,
		sensor_info int(11) NOT NULL,
		ip_module int(11) NOT NULL,
		threshold int(11) NOT NULL,
		sensor_history int(11) NOT NULL,
		PRIMARY KEY (uuid)
	) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
`)
	sqlCreatePermission, err := db.Query(query)
	if err != nil {
		log.Printf(" - create permission table err: %s", err)
		return err
	}
	log.Println(" - permission table OK")
	defer sqlCreatePermission.Close()

	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS firebase_token (
			token VARCHAR(255) NOT NULL,
			group_uuid VARCHAR(36) NOT NULL,
			email VARCHAR(255) DEFAULT NULL 
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateGroup, err = db.Query(query)
	if err != nil {
		log.Printf(" - create firebase_token table err: %s", err)
		return err
	}
	log.Println(" - group table OK")
	defer sqlCreateGroup.Close()

	initPermission()
	return nil
}

func initPermission() {
	uuidDefault := uuid.New()
	uuidMiddle1 := uuid.New()
	uuidMiddle2 := uuid.New()
	uuidUser := uuid.New()
	uuidMaster := uuid.New()

	var count int
	countQuery := "SELECT COUNT(*) FROM permission"
	err := db.QueryRow(countQuery).Scan(&count)
	if err != nil {
		log.Printf(" - query count from permission table err: %s", err)
		return
	}
	if count == 0 {
		insertQuery := fmt.Sprintf(`
			INSERT INTO permission (uuid, name, user, permission, settop_create, sensor_info, ip_module, threshold, sensor_history)
			VALUES 
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d),
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d),
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d),
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d),
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d);
		`,
			uuidDefault.String(), "default", 0, 0, 0, 0, 0, 0, 1,
			uuidMiddle1.String(), "중간계층1", 1, 1, 1, 1, 1, 1, 1,
			uuidMiddle2.String(), "중간계층2", 0, 0, 1, 1, 1, 1, 1,
			uuidUser.String(), "사용자", 0, 0, 0, 0, 0, 0, 1,
			uuidMaster.String(), "master", 1, 1, 1, 1, 1, 1, 1,
		)

		_, err := db.Exec(insertQuery)
		if err != nil {
			log.Printf(" - insert data into permission table err: %s", err)
			return
		}

		log.Println(" - data inserted into permission table")
	}
}

func initGroup() {
	uuidDefault := uuid.New()

	var count int
	countQuery := "SELECT COUNT(*) FROM group_"
	err := db.QueryRow(countQuery).Scan(&count)
	if err != nil {
		log.Printf(" - query count from group_ table err: %s", err)
		return
	}
	if count == 0 {
		insertQuery := fmt.Sprintf(`
			INSERT INTO group_ (uuid, name)
			VALUES 
			('%s', '%s');
		`,
			uuidDefault.String(), "master",
		)

		_, err := db.Exec(insertQuery)
		if err != nil {
			log.Printf(" - insert data into group table err: %s", err)
			return
		}

		log.Println(" - data inserted into group table")
	}
}
