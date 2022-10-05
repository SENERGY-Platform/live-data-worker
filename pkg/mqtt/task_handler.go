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

package mqtt

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/live-data-worker/pkg/auth"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/marshaller"
	"github.com/SENERGY-Platform/live-data-worker/pkg/shared"
	"github.com/SENERGY-Platform/live-data-worker/pkg/taskmanager"
	paho "github.com/eclipse/paho.mqtt.golang"
	"golang.org/x/exp/maps"
	"log"
)

type TaskHandler struct {
	client       *Client
	config       configuration.Config
	topicTasks   map[string][]taskmanager.Task
	marsh        *marshaller.Marshaller
	errorhandler func(err error)
}

func NewTaskHandler(ctx context.Context, client *Client, config configuration.Config, authentication *auth.Auth) (*TaskHandler, error) {
	marsh, err := marshaller.NewMarshaller(ctx, config, authentication)
	if err != nil {
		return nil, err
	}
	return &TaskHandler{client: client, config: config, topicTasks: map[string][]taskmanager.Task{}, marsh: marsh, errorhandler: defaultErrorHandler}, nil
}

func (h *TaskHandler) UpdateTasks(tasks []taskmanager.Task) {
	topicTasks := map[string][]taskmanager.Task{}
	for _, task := range tasks {
		topic := "event/" + task.Info.LocalDeviceId + "/" + task.Info.LocalServiceId
		topicTasks = shared.Apsert(topicTasks, topic, task)
	}
	missing, added := shared.GetMissingOrAddedElements(maps.Keys(h.topicTasks), maps.Keys(topicTasks))
	if len(missing) > 0 {
		go func() { // respect paho requirements
			err := h.client.Unsubscribe(missing...)
			if err != nil {
				h.errorhandler(err)
			}
		}()
	}
	for _, topic := range added {
		topic := topic
		go func() { // respect paho requirements
			err := h.client.Subscribe(topic, h.onDeviceMessage)
			if err != nil {
				h.errorhandler(err)
			}
		}()
	}
	h.topicTasks = topicTasks
	if h.config.Debug {
		b, _ := json.MarshalIndent(h.topicTasks, "", "  ")
		log.Println("[TASKS] " + string(b))
	}
}

func (h *TaskHandler) SetErrorHandler(errorhandler func(err error)) {
	h.errorhandler = errorhandler
}

func (h *TaskHandler) onDeviceMessage(_ paho.Client, message paho.Message) {
	value := string(message.Payload())
	deduplicate := make(map[string]uint8)
	for _, task := range h.topicTasks[message.Topic()] {
		_, ok := deduplicate[task.Info.DeviceId+task.Info.AspectId+task.Info.FunctionId+task.Info.ServiceId+task.Info.CharacteristicId]
		if ok {
			continue
		}
		marshalled, err := h.marsh.Resolve(task.Info.DeviceId, task.Info.AspectId, task.Info.FunctionId, task.Info.ServiceId, task.Info.CharacteristicId, value)
		b, _ := json.Marshal(marshalled)
		go func() { // respect paho requirements
			err = h.client.Publish(shared.GetOutputMqttTopic(h.config, task), string(b))
			if err != nil {
				h.errorhandler(err)
			}
		}()
		deduplicate[task.Info.DeviceId+task.Info.AspectId+task.Info.FunctionId+task.Info.ServiceId+task.Info.CharacteristicId] = 0
	}
}

func defaultErrorHandler(err error) {
	log.Println("[WARNING] No Error Handler configured")
	log.Println("[ERROR] ", err.Error())
}
