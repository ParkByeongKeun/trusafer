package main

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

var Conf Config

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
	} `json:"etc"`
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
	InfluxDB struct {
		Org         string `json:"org"`
		Bucket      string `json:"bucket"`
		Address     string `json:"address"`
		Token       string `json:"token"`
		Measurement string `json:"measurement"`
		IsLog       bool   `json:"is_log"`
	} `json:"influxdb"`
	Jwt struct {
		SecretKeyAT     string        `json:"secret_key_at"`
		TokenDurationAT time.Duration `json:"token_duration_at"`
		SecretKeyRT     string        `json:"secret_key_rt"`
		TokenDurationRT time.Duration `json:"token_duration_rt"`
	} `json:"jwt"`
	Encryption struct {
		Passphrase string `json:"passphrase"`
	} `json:"encryption"`
	Broker struct {
		UserId       string `json:"user_id"`
		UserPassword string `json:"user_password"`
		Address 		 string `json:"address"`
	} `json:"broker"`
}

func LoadConfiguration(file string) error {
	configFile, err := os.Open(file)
	defer configFile.Close()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	jsonParser := json.NewDecoder(configFile)
	jsonParser.Decode(&Conf)
	return nil
}
