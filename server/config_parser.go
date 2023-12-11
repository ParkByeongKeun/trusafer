package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type Config struct {
	Grpc struct {
		Port     int    `json:"port"`
		CertFile string `json:"cert_file"`
		KeyFile  string `json:"key_file"`
	} `json:"grpc"`
	Database struct {
		Id       string `json:"id"`
		Password string `json:"password"`
		Address  string `json:"address"`
		Name     string `json:"name"`
	} `json:"database"`
	Etc struct {
		SaveImageDir string `json:"save_image_dir"`
	} `json:"etc`
}

func LoadConfiguration(file string) (Config, error) {
	var conf Config
	configFile, err := os.Open(file)
	defer configFile.Close()
	if err != nil {
		fmt.Println(err.Error())
		return conf, err
	}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&conf)
	return conf, nil
}
