package main

import (
	"bufio"
	"database/sql"
	"log"
	"math/rand"
	"os"
	"path"
	"strings"
)

func getNullStringValidValue(nullstring sql.NullString) string {
	if nullstring.Valid {
		return nullstring.String
	}
	return ""
}

func getOnlyNumbers(s string) string {
	return strings.ReplaceAll(s, "-", "")
}

func saveJpegBytesImage(imgByte []byte, savePath string) (bool, error) {
	baseDir := path.Dir(savePath)
	info, err := os.Stat(baseDir)
	if err != nil || info.IsDir() {
		err = os.MkdirAll(baseDir, 0755)
		if err != nil {
			return false, err
		}
	}

	fo, err := os.Create(savePath)
	if err != nil {
		log.Println("os Create err")
		log.Println(err)
		return false, err
	}
	defer fo.Close()

	fw := bufio.NewWriter(fo)
	_, err = fw.Write(imgByte)
	if err != nil {
		log.Println("fw.Write err")
		log.Println(err)
		return false, err
	}

	return true, nil
}

func RandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rand.Intn(len(letters))]
	}
	return string(s)
}
