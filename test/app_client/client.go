package main

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"

	pb "github.com/ParkByeongKeun/trusafer-idl/maincontrol"

	"google.golang.org/grpc"
	// 프로토콜 버퍼 메시지가 정의된 파일의 패키지 경로로 대체하세요.
)

func main() {
	// 예제로 사용할 이미지 경로
	imagePath := "/Users/bkpark/Downloads/IMG_6153.jpg"
	// gRPC 서버 연결
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Could not connect: %v", err)
	}
	defer conn.Close()

	// gRPC 클라이언트 생성
	client := pb.NewFileServiceClient(conn)

	// 파일 서버로 파일 경로 전송
	response, err := client.SaveFile(context.Background(), &pb.FilePath{
		Path: imagePath,
	})
	if err != nil {
		log.Fatalf("Error saving file: %v", err)
	}

	// 서버 응답 출력
	fmt.Printf("Server response: %s\n", response.Path)
}

// 이미지 파일을 읽어 바이트 슬라이스로 반환
func readFile(filePath string) ([]byte, error) {
	imageBytes, err := ioutil.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return imageBytes, nil
}
