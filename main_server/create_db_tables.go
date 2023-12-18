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

	// create history
	query = fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS history (
			uuid VARCHAR(36) NOT NULL,
			sensor_serial VARCHAR(36) DEFAULT NULL,
			min_temp int(11) NOT NULL,
			max_temp int(11) NOT NULL,
			date date NOT NULL,
			PRIMARY KEY (uuid),
			KEY fk_history_trusafer (uuid)
		) ENGINE=InnoDB AUTO_INCREMENT=261 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateHistory, err := db.Query(query)
	if err != nil {
		log.Printf(" - create history err: %s", err)
		return err
	}
	log.Println(" - History table OK")
	defer sqlCreateHistory.Close()

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
			floor int(11) DEFAULT NULL,
			mac1 varchar(255) DEFAULT NULL,
			mac2 varchar(255) DEFAULT NULL,
			is_alive int(11) DEFAULT NULL,
			latest_version varchar(255) DEFAULT NULL,
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
			threshold_temp_warning varchar(255) DEFAULT NULL,
			threshold_temp_danger varchar(255) DEFAULT NULL,
			latest_version varchar(255) DEFAULT NULL,
			registered_time datetime DEFAULT NULL,
			mac varchar(255) DEFAULT NULL,
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
		CREATE TABLE IF NOT EXISTS group_ (
			uuid VARCHAR(36) NOT NULL,
			place_uuid VARCHAR(36) DEFAULT NULL,
			group_id int(11) DEFAULT NULL,
			name varchar(255) DEFAULT NULL,
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

	// query = fmt.Sprintf(`
	// 	CREATE TABLE IF NOT EXISTS ip_module (
	// 		id int(11) NOT NULL AUTO_INCREMENT,
	// 		settop_id int(11) DEFAULT NULL,
	// 		ip_address varchar(255) DEFAULT NULL,
	// 		mac_address varchar(255) DEFAULT NULL,
	// 		firmware_version varchar(255) DEFAULT NULL,
	// 		PRIMARY KEY (id)
	// 	) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	// `)
	// sqlCreateipModule, err := db.Query(query)
	// if err != nil {
	// 	log.Printf(" - create ipmodule table err: %s", err)
	// 	return err
	// }
	// log.Println(" - ipmodule table OK")
	// defer sqlCreateipModule.Close()

	query = fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS permission (
		uuid VARCHAR(36) NOT NULL,
		name varchar(255) UNIQUE DEFAULT NULL,
		user int(11) NOT NULL,
		permission int(11) NOT NULL,
		sensor_create int(11) NOT NULL,
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
	initPermission()
	return nil
}

func initPermission() {
	uuidDefault := uuid.New()
	uuidMiddle1 := uuid.New()
	uuidMiddle2 := uuid.New()
	uuidUser := uuid.New()

	var count int
	countQuery := "SELECT COUNT(*) FROM permission"
	err := db.QueryRow(countQuery).Scan(&count)
	if err != nil {
		log.Printf(" - query count from permission table err: %s", err)
		return
	}
	if count == 0 {
		insertQuery := fmt.Sprintf(`
			INSERT INTO permission (uuid, name, user, permission, sensor_create, sensor_info, ip_module, threshold, sensor_history)
			VALUES 
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d),
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d),
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d),
			('%s', '%s', %d, %d, %d, %d, %d, %d, %d);
		`,
			uuidDefault.String(), "default", 0, 0, 0, 0, 0, 0, 1,
			uuidMiddle1.String(), "중간계층1", 1, 1, 1, 1, 1, 1, 1,
			uuidMiddle2.String(), "중간계층2", 0, 0, 1, 1, 1, 1, 1,
			uuidUser.String(), "사용자", 0, 0, 0, 0, 0, 0, 1,
		)

		_, err := db.Exec(insertQuery)
		if err != nil {
			log.Printf(" - insert data into permission table err: %s", err)
			return
		}

		log.Println(" - data inserted into permission table")
	}
}
