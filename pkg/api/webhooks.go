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
		resp := SubscribeWebhookMsgResponse{
			Result: "ok",
			Topics: []WebhookmsgTopic{},
		}
		for _, topicReq := range msg.Topics {
			qos := topicReq.Qos
			if msg.Username != config.AuthClientId {
				_, _, _, _, _, _, _, err := shared.ParseSubscriptionTopic(config, topicReq.Topic, msg.Username, authentication)
				if err == nil {
					err := mqttClient.Publish(config.MqttSubscribeTopic, shared.GetLocalTime()+": "+msg.ClientId+" "+topicReq.Topic+" "+strconv.FormatInt(topicReq.Qos, 10)+" "+msg.Username)
					if err != nil {
						http.Error(writer, err.Error(), http.StatusInternalServerError)
						return
					}
				} else {
					qos = 128
				}
			}
			resp.Topics = append(resp.Topics, WebhookmsgTopic{
				Topic: topicReq.Topic,
				Qos:   qos,
			})
		}

		writer.Header().Set("Content-Type", "application/json")
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
		for _, topic := range msg.Topics {
			err := mqttClient.Publish(config.MqttUnsubscribeTopic, shared.GetLocalTime()+": "+msg.ClientId+" "+topic+" "+msg.Username)
			if err != nil {
				http.Error(writer, err.Error(), http.StatusInternalServerError)
				return
			}
		}
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
		err = mqttClient.Publish(config.MqttLogTopic, shared.GetLocalTime()+": "+msg.ClientId+" disconnected.")
		if err != nil {
			http.Error(writer, err.Error(), http.StatusInternalServerError)
			return
		}
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
		_, err = fmt.Fprint(writer, `{"result": "ok"}`)
		if err != nil {
			log.Println("ERROR: Could not write HTTP response " + err.Error())
		}
	})
}
