package main

import (
	"context"
	"encoding/json"
	"log"
	"time"

	pb "app_client/proto"
)

func prettyPrint(i interface{}) {
	s, _ := json.MarshalIndent(i, "", "\t")
	log.Print(string(s))
}

func testAddRegisterer() {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	registererRequest := pb.AddRegistererRequest{
		Registerer: &pb.Registerer{
			BarnId:      1,
			Name:        "Hello",
			PlateNumber: "55더7805",
			PhoneNumber: "010-4141-4141",
			Status:      pb.RegistererStatus_REGISTERER_STATUS_REGISTERED,
			Note:        "Hello",
		},
	}

	res, err := c.AddRegisterer(ctx, &registererRequest)
	if err != nil {
		log.Println(err)
	}
	log.Println("testAddRegisterer()")
	prettyPrint(res)
}

func testUpdateRegistererStatus(registerer_id uint64, status pb.RegistererStatus) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := c.GetRegisterer(ctx, &pb.GetRegistererRequest{RegistererId: registerer_id})
	if err != nil {
		log.Println(err)
	}

	log.Println("testUpdateRegistererStatus()")
	prettyPrint(res)

	registererRequest := pb.UpdateRegistererRequest{
		Registerer: &pb.Registerer{
			Id:           res.Registerer.Id,
			BarnId:       res.Registerer.BarnId,
			Name:         res.Registerer.Name,
			PlateNumber:  res.Registerer.PlateNumber,
			PhoneNumber:  res.Registerer.PhoneNumber,
			RegisterTime: res.Registerer.RegisterTime,
			Status:       status,
			Note:         "상태변경했음",
		},
	}

	_, err = c.UpdateRegisterer(ctx, &registererRequest)
	if err != nil {
		log.Println(err)
	}
	log.Println("Update success")
}

func testGetBarn() {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := c.GetBarn(ctx, &pb.GetBarnRequest{BarnId: 1})
	if err != nil {
		log.Println(err)
	}
	log.Println("testGetBarn()")
	prettyPrint(res)
}

func testGetBarnList() {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		res, err := c.GetBarnList(ctx, &pb.GetBarnListRequest{})
		if err != nil {
			log.Println(err)
		}
		log.Println("testGetBarnList()")
		prettyPrint(res)
	}
}

func testDeleteRegisterer(registerer_id uint64) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	for i := 0; i < 50; i++ {
		res, err := c.DeleteRegisterer(ctx, &pb.DeleteRegistererRequest{RegistererId: registerer_id})
		if err != nil {
			log.Println(err)
		}
		log.Println("testUpdateRegisterer()")
		prettyPrint(res)
	}
}

func testGetRegisterer(registerer_id uint64) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	res, err := c.GetRegisterer(ctx, &pb.GetRegistererRequest{RegistererId: registerer_id})
	if err != nil {
		log.Println(err)
	}
	log.Println("testGetBarn()")
	prettyPrint(res)
}

func testGetRegistererList(barn_id uint64) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		res, err := c.GetRegistererList(ctx, &pb.GetRegistererListRequest{BarnId: barn_id})
		if err != nil {
			log.Println(err)
		}
		log.Println("testGetBarnList()")
		prettyPrint(res)
	}
}

func testGetAccessLog(barn_id uint64) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		res, err := c.GetAccessLog(ctx, &pb.GetAccessLogRequest{BarnId: barn_id})
		if err != nil {
			log.Println(err)
		}
		log.Println("testGetBarnList()")
		prettyPrint(res)
	}
}

func testOpenGateFromApp(barn_id uint64) {
	defer wg.Done()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 50; i++ {
		res, err := c.OpenGateFromApp(ctx, &pb.OpenGateFromAppRequest{BarnId: barn_id})
		if err != nil {
			log.Println(err)
		}
		log.Println("testGetBarnList()")
		prettyPrint(res)
	}
}
