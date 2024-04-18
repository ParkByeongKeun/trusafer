// firebaseutil/firebaseutil.go

package firebaseutil

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"sync"

	firebase "firebase.google.com/go/v4"
	"firebase.google.com/go/v4/messaging"
	"golang.org/x/net/context"
	"google.golang.org/api/option"
)

var mu sync.Mutex

var (
	_firebase_client *messaging.Client
	_firebase_ctx    context.Context = context.Background()
)

type AccountKey struct {
	ProjectID string `json:"project_id"`
}

// 파이어베이스 초기화
func InitFirebase(serviceAccountKeyPath string) error {
	keyFile, err := ioutil.ReadFile(serviceAccountKeyPath)
	if err != nil {
		return err
	}

	accountKey := AccountKey{}
	if err := json.Unmarshal(keyFile, &accountKey); err != nil {
		return err
	}

	opt := option.WithCredentialsFile(serviceAccountKeyPath)
	config := &firebase.Config{ProjectID: accountKey.ProjectID}

	app, err := firebase.NewApp(_firebase_ctx, config, opt)
	if err != nil {
		return err
	}

	_firebase_client, err = app.Messaging(_firebase_ctx)
	if err != nil {
		return err
	}

	return nil
}

func SubscribeToTopic(tokens []string, topic string, isSubscribe bool) error {
	if isSubscribe {
		_, err := _firebase_client.SubscribeToTopic(_firebase_ctx, tokens, topic)
		if err != nil {
			log.Printf("Error subscribing to topic %s: %v", topic, err)
			return err
		}

		log.Printf("Tokens subscribed to topic %s", topic)
	} else {
		_, err := _firebase_client.UnsubscribeFromTopic(_firebase_ctx, tokens, topic)
		if err != nil {
			log.Printf("Error subscribing to topic %s: %v", topic, err)
			return err
		}

		log.Printf("Tokens subscribed to topic %s", topic)
	}

	return nil
}

func SendMessage(title, body, style, topic string) error {
	// mu.Lock()
	// defer mu.Unlock()

	message := &messaging.Message{
		Notification: &messaging.Notification{
			Title: title,
			Body:  body,
		},
		Data: map[string]string{
			"title": title,
			"body":  body,
			"style": style,
		},
		Topic: topic,
	}

	response, err := _firebase_client.Send(_firebase_ctx, message)
	if err != nil {
		log.Printf("Error sending message to topic %s: %v", topic, err)
		return err
	}

	log.Printf("Successfully sent message to topic %s. Message ID: %s", topic, response)
	return nil
}

func SendMessageAsync(title, body, style, topic string) {
	go func() {
		err := SendMessage(title, body, style, topic)
		if err != nil {
			log.Println("Error sending message:", err)
		}
	}()
}
