/*
 * Copyright 2022 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package api

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/SENERGY-Platform/live-data-worker/pkg/auth"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/taskmanager"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"strconv"
	"strings"
)

func init() {
	endpoints = append(endpoints, WebhookEndpoints)
}

func WebhookEndpoints(config configuration.Config, router *httprouter.Router, manager *taskmanager.Manager, authentication *auth.Auth) {
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
		tasks := []taskmanager.Task{}
		for _, topicReq := range msg.Topics {
			qos := topicReq.Qos
			deviceId, localDeviceId, serviceId, functionId, aspectId, characteristicId, err := parseSubscriptionTopic(config,
				topicReq.Topic, msg.Username, authentication)
			if err == nil {
				tasks = append(tasks, taskmanager.Task{
					Id: getTaskId(msg.ClientId, topicReq.Topic),
					Info: taskmanager.TaskInfo{
						DeviceId:         deviceId,
						LocalDeviceId:    localDeviceId,
						ServiceId:        serviceId,
						FunctionId:       functionId,
						AspectId:         aspectId,
						CharacteristicId: characteristicId,
					},
				})
			} else {
				qos = 128
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
		taskIds := []string{}
		for _, topic := range msg.Topics {
			taskIds = append(taskIds, getTaskId(msg.ClientId, topic))
		}
		manager.DeleteTasks(taskIds)
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
		manager.DeleteTasksWithIdPrefix(msg.ClientId)
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

func getTaskId(clientId string, topic string) string {
	return clientId + "_" + topic
}

func parseSubscriptionTopic(config configuration.Config, topic string, userId string, authentication *auth.Auth) (deviceId, localDeviceId,
	serviceId, functionId, aspectId, characteristicId string, err error) {

	if config.MqttPrefix != "" && !strings.HasPrefix(topic, config.MqttPrefix) {
		err = errors.New("topic missing prefix")
		return
	}
	topic = strings.TrimPrefix(topic, config.MqttPrefix)
	topicParts := strings.Split(topic, "/")
	if len(topicParts) != 5 {
		err = errors.New("unexpected number of topic parts")
		return
	}
	localDeviceId, serviceId, functionId, aspectId, characteristicId = topicParts[0], topicParts[1], topicParts[2], topicParts[3], topicParts[4]

	token, err := authentication.GetCachedUserToken(userId)
	if err != nil {
		return
	}
	resp, err := token.Get(config.PermissionsUrl + "/v3/resources/devices?rights=x&filter=local_id=" + localDeviceId)
	if err != nil {
		return
	}
	if resp.StatusCode > 299 {
		err = errors.New("unexpected statuscode " + strconv.Itoa(resp.StatusCode))
		return
	}
	var devices []Device
	err = json.NewDecoder(resp.Body).Decode(&devices)
	if err != nil {
		return
	}
	if len(devices) != 1 {
		err = errors.New("unexpected number of devices received from permSearch")
		return
	}
	deviceId = devices[0].Id
	return
}
