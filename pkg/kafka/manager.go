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
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/mqtt"
	"github.com/SENERGY-Platform/live-data-worker/pkg/taskmanager"
	"golang.org/x/exp/maps"
	"log"
	"strings"
	"sync"
	"time"
)

type Manager struct {
	ctx        context.Context
	cancel     context.CancelFunc
	wg         *sync.WaitGroup
	config     configuration.Config
	publisher  *mqtt.Publisher
	topicTasks map[string][]taskmanager.Task
	consumer   *Consumer
}

func New(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, publisher *mqtt.Publisher) *Manager {
	return &Manager{
		ctx:        ctx,
		wg:         wg,
		config:     config,
		publisher:  publisher,
		topicTasks: map[string][]taskmanager.Task{},
	}
}

func (m *Manager) UpdateTasks(tasks []taskmanager.Task) {
	topicTasks := map[string][]taskmanager.Task{}
	for _, task := range tasks {
		topic := strings.ReplaceAll(task.Info.ServiceId, ":", "_")
		topicTasks = apsert(topicTasks, topic, task)
	}
	if m.consumer == nil || !equalStringSlice(maps.Keys(topicTasks), maps.Keys(m.topicTasks)) || !equalStringSlice(m.consumer.topics, maps.Keys(m.topicTasks)) {
		if m.cancel != nil {
			m.cancel()
		}
		m.wg.Wait()
		ctx, cancel := context.WithCancel(m.ctx)
		m.cancel = cancel
		consumer, err := newConsumer(ctx, m.wg, m.config.KafkaUrl, maps.Keys(topicTasks), m.config.KafkaConsumerGroup, Latest, m.onMessage, m.onError, m.config.Debug)
		if err != nil {
			panic(err)
		}
		m.consumer = consumer
	}
	m.topicTasks = topicTasks
}

func (m *Manager) onMessage(topic string, msg []byte, _ time.Time) error {
	tasks, ok := m.topicTasks[topic]
	if !ok {
		return nil
	}
	var msgDecoded DeviceMessage
	err := json.Unmarshal(msg, &msgDecoded)
	if err != nil {
		return err
	}
	for _, task := range tasks {
		if msgDecoded.DeviceId != task.Info.DeviceId {
			continue
		}
		// TODO marshal
		b, _ := json.Marshal(msgDecoded.Value) // TODO
		marshalled := string(b)                // TODO
		err = m.publisher.Publish(m.config.MqttPrefix+task.Info.LocalDeviceId+"/"+task.Info.ServiceId+"/"+task.Info.AspectId+"/"+task.Info.CharacteristicId, marshalled)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Manager) onError(err error, _ *Consumer) {
	log.Println("ERROR: Kafka Consumer: " + err.Error() + "\n Restarting Consumer...")
	m.cancel()
	m.wg.Wait()
	m.consumer = nil
	tasks := []taskmanager.Task{}
	for _, t := range m.topicTasks {
		tasks = append(tasks, t...)
	}
	m.UpdateTasks(tasks)
}

func apsert(m map[string][]taskmanager.Task, key string, task taskmanager.Task) map[string][]taskmanager.Task {
	arr, ok := m[key]
	if !ok {
		arr = []taskmanager.Task{}
	}
	arr = append(arr, task)
	m[key] = arr
	return m
}

func equalStringSlice(a []string, b []string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a) != len(b) {
		return false
	}
	for _, aElem := range a {
		if !elemInSlice(aElem, b) {
			return false
		}
	}
	return true
}

func elemInSlice(elem string, slice []string) bool {
	for _, sliceElem := range slice {
		if elem == sliceElem {
			return true
		}
	}
	return false
}
