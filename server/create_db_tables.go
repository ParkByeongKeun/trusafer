package main

import (
	"fmt"
	"log"
)

func createDBTablesIfNotExist() error {

	log.Println("Create db tables if not exist")

	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS place (
			id int(11) NOT NULL AUTO_INCREMENT,
			name varchar(50) DEFAULT NULL,
			address varchar(255) DEFAULT NULL,
			owner_id int(11) DEFAULT NULL,
			registered_time datetime DEFAULT NULL,
			PRIMARY KEY (id),
			KEY trusafer_id_IDX (id,name,address,owner_id,registered_time) USING BTREE
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
			id int(11) NOT NULL AUTO_INCREMENT,
			sensor_id int(11) NOT NULL,
			temp varchar(255) NOT NULL,
			date datetime NOT NULL,
			PRIMARY KEY (id),
			KEY fk_history_trusafer (trusafer_id),
			CONSTRAINT fk_history_trusafer FOREIGN KEY (trusafer_id) REFERENCES trusafer (id)
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
			id int(11) NOT NULL AUTO_INCREMENT,
			auth_id int(11) NOT NULL,
			company_name varchar(20) NOT NULL,
			company_number varchar(20) NOT NULL,
			company_number_file_path varchar(20) NOT NULL,
			status smallint(5) unsigned DEFAULT 0,
			alarm smallint(5) unsigned DEFAULT 0,
			auth int(11) NOT NULL,
			group_id int(11) NOT NULL,
			PRIMARY KEY (id),
			UNIQUE KEY registerer_UN (trusafer_id,auth_id),
			CONSTRAINT fk_registerer_trusafer FOREIGN KEY (trusafer_id) REFERENCES trusafer (id)
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
			id int(11) NOT NULL AUTO_INCREMENT,
			place_id varchar(50) DEFAULT NULL,
			serial varchar(255) DEFAULT NULL,
			ip_address varchar(255) DEFAULT NULL,
			location varchar(255) DEFAULT NULL,
			floor int(11) DEFAULT NULL,
			ip_module varchar(255) DEFAULT NULL,
			registered_time datetime DEFAULT NULL,
			PRIMARY KEY (id),
			KEY trusafer_id_IDX (id,place_id,serial,ip_address,location,floor,ip_module,registered_time) USING BTREE
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
			id int(11) NOT NULL AUTO_INCREMENT,
			settop_id varchar(50) DEFAULT NULL,
			status varchar(255) DEFAULT NULL,
			serial varchar(255) DEFAULT NULL,
			ip_address varchar(255) DEFAULT NULL,
			location varchar(255) DEFAULT NULL,
			threshold_temp varchar(255) DEFAULT NULL,
			version varchar(255) DEFAULT NULL,
			registered_time datetime DEFAULT NULL,
			PRIMARY KEY (id),
			KEY trusafer_id_IDX (id,settop_id,status,serial,ip_address,location,threshold_temp,version,registered_time) USING BTREE
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
		CREATE TABLE IF NOT EXISTS group (
			id int(11) NOT NULL AUTO_INCREMENT,
			place_id varchar(50) DEFAULT NULL,
			group_id varchar(255) DEFAULT NULL,
			name varchar(255) DEFAULT NULL,
			PRIMARY KEY (id),
			KEY trusafer_id_IDX (id,place_id,group_id,name) USING BTREE
		) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4;
	`)
	sqlCreateGroup, err := db.Query(query)
	if err != nil {
		log.Printf(" - create sensor table err: %s", err)
		return err
	}
	log.Println(" - sensor table OK")
	defer sqlCreateGroup.Close()

	return nil
}
