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
	"github.com/SENERGY-Platform/live-data-worker/pkg/auth"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/shared"
	"github.com/SENERGY-Platform/live-data-worker/pkg/taskmanager"
	paho "github.com/eclipse/paho.mqtt.golang"
	"log"
	"regexp"
	"sync"
)

type Manager struct {
	Client         *Client
	tasks          *taskmanager.Manager
	authentication *auth.Auth
	config         configuration.Config
	subscribeRgx   *regexp.Regexp
	unsubscribeRgx *regexp.Regexp
	disconnectRgx  *regexp.Regexp
}

func NewManager(ctx context.Context, wg *sync.WaitGroup, config configuration.Config, tasks *taskmanager.Manager, authentication *auth.Auth) (manager *Manager, err error) {
	client, err := newClient(ctx, wg, config)
	if err != nil {
		return
	}
	manager = &Manager{
		Client:         client,
		tasks:          tasks,
		authentication: authentication,
		config:         config,
		subscribeRgx:   regexp.MustCompile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}: (\\S+) \\d (\\S+)\\s*(.*)"),
		unsubscribeRgx: regexp.MustCompile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}: (\\S+) (\\S+)\\s*(.*)"),
		disconnectRgx:  regexp.MustCompile("\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}: Client (\\S+) disconnected\\."),
	}
	return
}

func (m *Manager) Init() (err error) {
	err = m.Client.Subscribe(m.config.MqttSubscribeTopic, m.onSubscribe)
	if err != nil {
		return
	}
	err = m.Client.Subscribe(m.config.MqttUnsubscribeTopic, m.onUnsubscribe)
	if err != nil {
		return
	}
	err = m.Client.Subscribe(m.config.MqttLogTopic, m.onLog)
	if err != nil {
		return
	}
	return err
}

func (m *Manager) onSubscribe(_ paho.Client, message paho.Message) {
	msg := string(message.Payload())
	if m.config.Debug {
		log.Println("[MQTT] " + message.Topic() + ": " + msg)
	}
	matches := m.subscribeRgx.FindStringSubmatch(msg)
	if len(matches) < 3 {
		if m.config.Debug {
			log.Println("[MQTT] Ignoring message: Does not match subscribe regexp")
		}
		return
	}
	clientId, topic := matches[1], matches[2]
	username := ""
	if len(matches) >= 4 {
		username = matches[3]
	}
	deviceId, localDeviceId, serviceId, localServiceId, functionId, aspectId, characteristicId, err := shared.ParseSubscriptionTopic(m.config,
		topic, username, m.authentication)
	if err != nil {
		if m.config.Debug {
			log.Println(err.Error())
		}
		return
	}
	task := taskmanager.Task{
		Id: shared.GetTaskId(clientId, topic),
		Info: taskmanager.TaskInfo{
			DeviceId:         deviceId,
			LocalDeviceId:    localDeviceId,
			ServiceId:        serviceId,
			LocalServiceId:   localServiceId,
			FunctionId:       functionId,
			AspectId:         aspectId,
			CharacteristicId: characteristicId,
		},
	}
	m.tasks.AddTask(task)
}

func (m *Manager) onUnsubscribe(_ paho.Client, message paho.Message) {
	msg := string(message.Payload())
	if m.config.Debug {
		log.Println("[MQTT] " + message.Topic() + ": " + msg)
	}
	matches := m.unsubscribeRgx.FindStringSubmatch(msg)
	if len(matches) < 3 {
		if m.config.Debug {
			log.Println("[MQTT] Ignoring message: Does not match unsubscribe regexp")
		}
		return
	}
	clientId, topic := matches[1], matches[2]
	m.tasks.DeleteTask(shared.GetTaskId(clientId, topic))
}

func (m *Manager) onLog(_ paho.Client, message paho.Message) {
	msg := string(message.Payload())
	if m.config.Debug {
		log.Println("[MQTT] " + message.Topic() + ": " + msg)
	}
	matches := m.disconnectRgx.FindStringSubmatch(msg)
	if len(matches) < 2 {
		if m.config.Debug {
			log.Println("[MQTT] Ignoring message: Does not match disconnect regexp")
		}
		return
	}
	clientId := matches[1]
	m.tasks.DeleteTasksWithIdPrefix(clientId)
}
