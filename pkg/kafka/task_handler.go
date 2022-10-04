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

package kafka

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/live-data-worker/pkg/auth"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/marshaller"
	"github.com/SENERGY-Platform/live-data-worker/pkg/mqtt"
	"github.com/SENERGY-Platform/live-data-worker/pkg/shared"
	"github.com/SENERGY-Platform/live-data-worker/pkg/taskmanager"
	"golang.org/x/exp/maps"
	"log"
	"strings"
	"sync"
	"time"
)

type TaskHandler struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
	config     configuration.Config
	mqttClient *mqtt.Client
	topicTasks map[string][]taskmanager.Task
	consumer   *Consumer
	marsh      *marshaller.Marshaller
}

func NewTaskHandler(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, mqttClient *mqtt.Client, authentication *auth.Auth) (*TaskHandler, error) {
	marsh, err := marshaller.NewMarshaller(ctx, config, authentication)
	if err != nil {
		return nil, err
	}
	return &TaskHandler{
		ctx:        ctx,
		wg:         wg,
		config:     config,
		mqttClient: mqttClient,
		topicTasks: map[string][]taskmanager.Task{},
		marsh:      marsh,
	}, nil
}

func (h *TaskHandler) UpdateTasks(tasks []taskmanager.Task) {
	topicTasks := map[string][]taskmanager.Task{}
	for _, task := range tasks {
		topic := strings.ReplaceAll(task.Info.ServiceId, ":", "_")
		topicTasks = shared.Apsert(topicTasks, topic, task)
	}
	if h.consumer == nil || !shared.EqualStringSliceIgnoreOrder(maps.Keys(topicTasks), maps.Keys(h.topicTasks)) ||
		!shared.EqualStringSliceIgnoreOrder(h.consumer.topics, maps.Keys(h.topicTasks)) {

		if h.cancel != nil {
			h.cancel()
		}
		h.wg.Wait()
		ctx, cancel := context.WithCancel(h.ctx)
		h.cancel = cancel
		consumer, err := newConsumer(ctx, h.wg, h.config.KafkaUrl, maps.Keys(topicTasks), h.config.KafkaConsumerGroup,
			Latest, h.onMessage, h.onError, h.config.Debug)
		if err != nil {
			panic(err)
		}
		h.consumer = consumer
	}
	h.topicTasks = topicTasks
	if h.config.Debug {
		b, _ := json.MarshalIndent(h.topicTasks, "", "  ")
		log.Println("[TASKS] " + string(b))
	}
}

func (h *TaskHandler) onMessage(topic string, msg []byte, _ time.Time) error {
	tasks, ok := h.topicTasks[topic]
	if !ok {
		return nil
	}
	var msgDecoded DeviceMessage
	err := json.Unmarshal(msg, &msgDecoded)
	if err != nil {
		return err
	}
	deduplicate := make(map[string]uint8)
	for _, task := range tasks {
		if msgDecoded.DeviceId != task.Info.DeviceId {
			continue
		}
		_, ok := deduplicate[task.Info.DeviceId+task.Info.AspectId+task.Info.FunctionId+task.Info.ServiceId+task.Info.CharacteristicId]
		if ok {
			continue
		}
		marshalled, err := h.marsh.Resolve(task.Info.DeviceId, task.Info.AspectId, task.Info.FunctionId, task.Info.ServiceId, task.Info.CharacteristicId, msgDecoded.Value)
		b, _ := json.Marshal(marshalled)
		err = h.mqttClient.Publish(shared.GetOutputMqttTopic(h.config, task), string(b))
		if err != nil {
			return err
		}
		deduplicate[task.Info.DeviceId+task.Info.AspectId+task.Info.FunctionId+task.Info.ServiceId+task.Info.CharacteristicId] = 0
	}

	return nil
}

func (h *TaskHandler) onError(err error, _ *Consumer) {
	log.Println("ERROR: Kafka Consumer: " + err.Error() + "\n Restarting Consumer...")
	h.cancel()
	h.wg.Wait()
	h.consumer = nil
	tasks := []taskmanager.Task{}
	for _, t := range h.topicTasks {
		tasks = append(tasks, t...)
	}
	h.UpdateTasks(tasks)
}
