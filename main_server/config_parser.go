package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
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
	Gw struct {
		Port int `json:"port"`
	} `json:"gw"`
	Im struct {
		Port int `json:"port"`
	} `json:"im"`
	Log struct {
		Dir        string `json:"dir"`
		File       string `json:"file"`
		MaxSize    int    `json:"max_size"`
		MaxBackups int    `json:"max_backups"`
		MaxAge     int    `json:"max_age"`
		LocalTime  bool   `json:"local_time"`
		Compress   bool   `json:"compress"`
	} `json:"log"`
	Broker_address struct {
		Address string `json:"address"`
		Port    string `json:"port"`
	} `json:"broker_address"`
	Jwt struct {
		SecretKeyAT     string        `json:"secret_key_at"`
		TokenDurationAT time.Duration `json:"token_duration_at"`
		SecretKeyRT     string        `json:"secret_key_rt"`
		TokenDurationRT time.Duration `json:"token_duration_rt"`
	} `json:"jwt"`
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
