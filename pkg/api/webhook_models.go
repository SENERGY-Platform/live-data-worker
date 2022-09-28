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

type PublishWebhookMsg struct {
	Username string `json:"username"`
	ClientId string `json:"client_id"`
	Topic    string `json:"topic"`
	Payload  string `json:"payload"`
	Qos      int    `json:"qos"`
}

type WebhookmsgTopic struct {
	Topic string `json:"topic"`
	Qos   int64  `json:"qos"`
}

type SubscribeWebhookMsg struct {
	Username string            `json:"username"`
	ClientId string            `json:"client_id"`
	Topics   []WebhookmsgTopic `json:"topics"`
}

type SubscribeWebhookMsgResponse struct {
	Result string            `json:"result"`
	Topics []WebhookmsgTopic `json:"topics,omitempty"`
}

type UnsubscribeWebhookMsg struct {
	Username string   `json:"username"`
	ClientId string   `json:"client_id"`
	Topics   []string `json:"topics"`
}

type LoginWebhookMsg struct {
	Username     string `json:"username"`
	Password     string `json:"password"`
	ClientId     string `json:"client_id"`
	CleanSession bool   `json:"clean_session"`
}

type OnlineWebhookMsg struct {
	ClientId string `json:"client_id"`
}

type DisconnectWebhookMsg struct {
	ClientId string `json:"client_id"`
}

type Device struct {
	Id      string `json:"id"`
	LocalId string `json:"local_id"`
}
