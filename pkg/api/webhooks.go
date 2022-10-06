package api

import (
	"encoding/json"
	"fmt"
	"github.com/SENERGY-Platform/live-data-worker/pkg/auth"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/mqtt"
	"github.com/SENERGY-Platform/live-data-worker/pkg/shared"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"strconv"
	"sync"
)

func init() {
	endpoints = append(endpoints, WebhookEndpoints)
}

func WebhookEndpoints(config configuration.Config, router *httprouter.Router, authentication *auth.Auth, mqttClient *mqtt.Client) {
	router.POST("/login", func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		msg := LoginWebhookMsg{}
		err := json.NewDecoder(req.Body).Decode(&msg)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		if msg.Username != config.AuthClientId {
			token, err := authentication.GetUserToken(msg.Username, msg.Password)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusUnauthorized)
				return
			}
			if token == "" {
				http.Error(writer, err.Error(), http.StatusUnauthorized)
				return
			}
		} else if msg.Password != config.AuthClientSecret {
			http.Error(writer, err.Error(), http.StatusUnauthorized)
			return
		}
		if msg.CleanSession == false {
			http.Error(writer, "only clean session allowed", http.StatusBadRequest)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, err = fmt.Fprint(writer, `{"result": "ok"}`)
		if err != nil {
			log.Println("ERROR: Could not write HTTP response " + err.Error())
		}
	})

	router.POST("/subscribe", func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		msg := SubscribeWebhookMsg{}
		err := json.NewDecoder(req.Body).Decode(&msg)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		if mqttClient == nil || !mqttClient.Ready() {
			http.Error(writer, "mqttClient not initialized", http.StatusServiceUnavailable)
			return
		}
		resp := SubscribeWebhookMsgResponse{
			Result: "ok",
			Topics: make([]WebhookmsgTopic, len(msg.Topics)),
		}
		wg := sync.WaitGroup{}
		wg.Add(len(msg.Topics))
		for i, topicReq := range msg.Topics {
			i := i
			topicReq := topicReq
			go func() {
				qos := topicReq.Qos
				if msg.Username != config.AuthClientId {
					_, _, _, _, _, _, _, err := shared.ParseSubscriptionTopic(config, topicReq.Topic, msg.Username, authentication)
					if err == nil {
						err := mqttClient.Publish(config.MqttSubscribeTopic, shared.GetLocalTime()+": "+msg.ClientId+" "+strconv.FormatInt(topicReq.Qos, 10)+" "+topicReq.Topic+" "+msg.Username)
						if err != nil {
							http.Error(writer, err.Error(), http.StatusInternalServerError)
							return
						}
					} else {
						qos = 128 // "not allowed or possible"
					}
				}
				resp.Topics[i] = WebhookmsgTopic{
					Topic: topicReq.Topic,
					Qos:   qos,
				}
				wg.Done()
			}()
		}
		wg.Wait()
		if config.Debug {
			b, _ := json.Marshal(resp)
			log.Println(string(b))
		}

		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		err = json.NewEncoder(writer).Encode(resp)
		if err != nil {
			log.Println("ERROR: Could not write HTTP response " + err.Error())
		}

	})

	router.POST("/unsubscribe", func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		msg := UnsubscribeWebhookMsg{}
		err := json.NewDecoder(req.Body).Decode(&msg)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		if mqttClient == nil || !mqttClient.Ready() {
			http.Error(writer, "mqttClient not initialized", http.StatusServiceUnavailable)
			return
		}
		for _, topic := range msg.Topics {
			err := mqttClient.Publish(config.MqttUnsubscribeTopic, shared.GetLocalTime()+": "+msg.ClientId+" "+topic+" "+msg.Username)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, err = fmt.Fprint(writer, `{"result": "ok"}`)
		if err != nil {
			log.Println("ERROR: Could not write HTTP response " + err.Error())
		}
	})

	router.POST("/disconnect", func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		msg := DisconnectWebhookMsg{}
		err := json.NewDecoder(req.Body).Decode(&msg)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		if mqttClient == nil || !mqttClient.Ready() {
			http.Error(writer, "mqttClient not initialized", http.StatusServiceUnavailable)
			return
		}
		err = mqttClient.Publish(config.MqttLogTopic, shared.GetLocalTime()+": "+msg.ClientId+" disconnected.")
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, err = fmt.Fprint(writer, `{}`)
		if err != nil {
			log.Println("ERROR: Could not write HTTP response " + err.Error())
		}
	})

	router.POST("/publish", func(writer http.ResponseWriter, req *http.Request, _ httprouter.Params) {
		msg := PublishWebhookMsg{}
		err := json.NewDecoder(req.Body).Decode(&msg)
		if err != nil {
			http.Error(writer, err.Error(), http.StatusBadRequest)
			return
		}
		if msg.Username != config.AuthClientId {
			http.Error(writer, err.Error(), http.StatusUnauthorized)
			return
		}
		writer.Header().Set("Content-Type", "application/json; charset=utf-8")
		_, err = fmt.Fprint(writer, `{"result": "ok"}`)
		if err != nil {
			log.Println("ERROR: Could not write HTTP response " + err.Error())
		}
	})
}
