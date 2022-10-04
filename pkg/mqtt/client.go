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
	"errors"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/hashicorp/go-uuid"
	"log"
	"sync"
)

type Client struct {
	client paho.Client
	qos    byte
	debug  bool
}

func newClient(ctx context.Context, wg *sync.WaitGroup, config configuration.Config) (mqtt *Client, err error) {
	mqtt = &Client{debug: config.Debug, qos: uint8(config.MqttQos)}
	clientIdSuffix, err := uuid.GenerateUUID()
	if err != nil {
		return nil, err
	}
	options := paho.NewClientOptions().
		SetPassword(config.MqttPw).
		SetUsername(config.MqttUser).
		SetAutoReconnect(true).
		SetCleanSession(true).
		SetClientID(config.MqttClientId + "_" + clientIdSuffix).
		AddBroker("tcp://" + config.MqttHost + ":" + config.MqttPort)

	mqtt.client = paho.NewClient(options)
	if token := mqtt.client.Connect(); token.Wait() && token.Error() != nil {
		log.Println("Error on Client.Connect(): ", token.Error())
		return mqtt, token.Error()
	}
	log.Println("MQTT client up and running...")
	wg.Add(1)
	go func() {
		<-ctx.Done()
		mqtt.client.Disconnect(0)
		wg.Done()
	}()

	return mqtt, nil
}

func (c *Client) Publish(topic string, msg string) (err error) {
	if !c.client.IsConnected() {
		return errors.New("mqtt client not connected")
	}
	token := c.client.Publish(topic, c.qos, false, msg)
	if c.debug {
		log.Printf("[MQTT] Publish %v: %v", topic, msg)
	}
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return err
}

func (c *Client) Subscribe(topic string, handler paho.MessageHandler) (err error) {
	if !c.client.IsConnected() {
		return errors.New("mqtt client not connected")
	}
	token := c.client.Subscribe(topic, c.qos, handler)
	if c.debug {
		log.Printf("[MQTT] Subscribe %v", topic)
	}
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return err
}

func (c *Client) Unsubscribe(topics ...string) (err error) {
	token := c.client.Unsubscribe(topics...)
	if c.debug {
		log.Printf("[MQTT] Unsubscribe %v", topics)
	}
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return err
}
