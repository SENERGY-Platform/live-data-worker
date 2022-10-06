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
	"github.com/segmentio/kafka-go"
	"golang.org/x/exp/maps"
	"io"
	"log"
	"strings"
	"sync"
	"time"
)

type TaskHandler struct {
	ctx            context.Context
	config         configuration.Config
	mqttClient     *mqtt.Client
	topicTasks     map[string][]taskmanager.Task
	marsh          *marshaller.Marshaller
	errorhandler   func(err error)
	topicReader    map[string]readerContext
	topicReaderMux sync.Mutex
}

type readerContext struct {
	reader *kafka.Reader
	cancel context.CancelFunc
}

func NewTaskHandler(ctx context.Context, config configuration.Config, mqttClient *mqtt.Client, authentication *auth.Auth) (*TaskHandler, error) {
	marsh, err := marshaller.NewMarshaller(ctx, config, authentication)
	if err != nil {
		return nil, err
	}
	return &TaskHandler{
		ctx:            ctx,
		config:         config,
		mqttClient:     mqttClient,
		topicTasks:     map[string][]taskmanager.Task{},
		marsh:          marsh,
		errorhandler:   defaultErrorHandler,
		topicReader:    map[string]readerContext{},
		topicReaderMux: sync.Mutex{},
	}, nil
}

func (h *TaskHandler) UpdateTasks(tasks []taskmanager.Task) {
	topicTasks := map[string][]taskmanager.Task{}
	for _, task := range tasks {
		topic := strings.ReplaceAll(task.Info.ServiceId, ":", "_")
		topicTasks = shared.Apsert(topicTasks, topic, task)
	}
	missing, added := shared.GetMissingOrAddedElements(maps.Keys(h.topicReader), maps.Keys(topicTasks))
	wg := sync.WaitGroup{}
	wg.Add(len(missing) + len(added))
	for _, topic := range missing {
		topic := topic
		go func() {
			h.removeReader(topic)
			wg.Done()
		}()
	}
	for _, topic := range added {
		topic := topic
		go func() {
			h.addReader(topic)
			wg.Done()
		}()
	}
	wg.Wait()
	h.topicTasks = topicTasks
	if h.config.Debug {
		b, _ := json.MarshalIndent(h.topicTasks, "", "  ")
		log.Println("[TASKS] " + string(b))
	}
}

func (h *TaskHandler) SetErrorHandler(errorhandler func(err error)) {
	h.errorhandler = errorhandler
}

func (h *TaskHandler) onMessage(topic string, msg []byte, _ time.Time) error {
	tasks, ok := h.topicTasks[topic]
	if !ok {
		log.Println("[WARNING]", "Got message on topic, but no active task", topic)
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

func (h *TaskHandler) addReader(topic string) {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{h.config.KafkaUrl},
		GroupID:     h.config.KafkaConsumerGroup,
		Topic:       topic,
		MaxWait:     500 * time.Millisecond,
		StartOffset: kafka.LastOffset,
	})
	ctx, cancel := context.WithCancel(h.ctx)
	h.topicReaderMux.Lock()
	h.topicReader[topic] = readerContext{
		reader: r,
		cancel: cancel,
	}
	h.topicReaderMux.Unlock()
	go func() {
		for {
			m, err := r.FetchMessage(ctx)
			if err != nil {
				if err == io.EOF || err == context.Canceled {
					return
				}
				h.errorhandler(err)
				return
			}
			err = h.onMessage(m.Topic, m.Value, m.Time)
			if err != nil {
				if err == io.EOF || err == context.Canceled {
					return
				}
				h.errorhandler(err)
				return
			}
			err = r.CommitMessages(ctx, m)
			if err != nil {
				if err == io.EOF || err == context.Canceled {
					return
				}
				h.errorhandler(err)
				return
			}
		}
	}()
}

func (h *TaskHandler) removeReader(topic string) {
	reader, ok := h.topicReader[topic]
	if !ok {
		return
	}
	reader.cancel()
	err := reader.reader.Close()
	if err != nil {
		h.errorhandler(err)
		return
	}
	h.topicReaderMux.Lock()
	delete(h.topicReader, topic)
	h.topicReaderMux.Unlock()
}

func defaultErrorHandler(err error) {
	log.Println("[WARNING] No Error Handler configured")
	log.Println("[ERROR] ", err.Error())
}
