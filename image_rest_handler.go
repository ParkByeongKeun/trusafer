package main

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "POST":
		textValue := r.FormValue("company")
		userFolder := textValue
		err := os.MkdirAll(userFolder, os.ModePerm)
		if err != nil {
			log.Println("error creating user folder", err)
			err = status.Errorf(codes.InvalidArgument, "Bad Request: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		imageFileName := "company_logo" + ".jpg"
		imageFilePath := filepath.Join(userFolder, imageFileName)
		file, _, err := r.FormFile("file_image")
		if err != nil {
			log.Println("error retrieving file", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer file.Close()

		dst, err := os.Create(imageFilePath)
		if err != nil {
			log.Println("error creating file", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		defer dst.Close()
		if _, err := io.Copy(dst, file); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		fmt.Fprintf(w, "uploaded file: %s", imageFilePath)
	}
}

func imageHandler(w http.ResponseWriter, r *http.Request) {
	textValue := r.FormValue("company")
	filePath := textValue + "/company_logo.jpg"

	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer file.Close()

	w.Header().Set("Content-Type", "image/jpeg")
	if _, err := io.Copy(w, file); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}
