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

package configuration

import (
	"encoding/json"
	"fmt"
	dc_conf "github.com/SENERGY-Platform/device-command/pkg/configuration"
	"log"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type Config struct {
	MgwMode bool `json:"mgw_mode"`

	ServerPort          string `json:"server_port"`
	MarshallerUrl       string `json:"marshaller_url"`
	PermissionsUrl      string `json:"permissions_url"`
	DeviceManagerUrl    string `json:"device_manager_url"`
	DeviceRepositoryUrl string `json:"device_repository_url"`

	KafkaUrl           string `json:"kafka_url"`
	KafkaConsumerGroup string `json:"kafka_consumer_group"`

	MqttHost             string `json:"mqtt_host"`
	MqttPort             string `json:"mqtt_port"`
	MqttClientId         string `json:"mqtt_client_id"`
	MqttUser             string `json:"mqtt_user"`
	MqttPw               string `json:"mqtt_pw"`
	MqttPrefix           string `json:"mqtt_prefix"`
	MqttQos              int    `json:"mqtt_qos"`
	MqttSubscribeTopic   string `json:"mqtt_subscribe_topic"`
	MqttUnsubscribeTopic string `json:"mqtt_unsubscribe_topic"`
	MqttLogTopic         string `json:"mqtt_log_topic"`

	AuthEndpoint     string `json:"auth_endpoint"`
	AuthClientId     string `json:"auth_client_id"`
	AuthClientSecret string `json:"auth_client_secret"`
	AuthUserName     string `json:"auth_user_name"`
	AuthPassword     string `json:"auth_password"`

	Debug bool `json:"debug"`
}

func (config Config) ToDcConf() dc_conf.Config {
	return dc_conf.Config{
		MarshallerUrl:                 config.MarshallerUrl,
		DeviceManagerUrl:              config.DeviceManagerUrl,
		DeviceRepositoryUrl:           config.DeviceRepositoryUrl,
		PermissionsUrl:                config.PermissionsUrl,
		UseIotFallback:                false,
		AuthEndpoint:                  config.AuthEndpoint,
		AuthClientId:                  config.AuthClientId,
		AuthUserName:                  config.AuthUserName,
		AuthPassword:                  config.AuthPassword,
		MgwConceptRepoRefreshInterval: 3600,
		IotFallbackFile:               "devicerepo_fallback.json",
	}
}

// loads config from json in location and used environment variables (e.g ZookeeperUrl --> ZOOKEEPER_URL)
func Load(location string) (config Config, err error) {
	file, error := os.Open(location)
	if error != nil {
		log.Println("error on config load: ", error)
		return config, error
	}
	decoder := json.NewDecoder(file)
	error = decoder.Decode(&config)
	if error != nil {
		log.Println("invalid config json: ", error)
		return config, error
	}
	handleEnvironmentVars(&config)
	return config, err
}

var camel = regexp.MustCompile("(^[^A-Z]*|[A-Z]*)([A-Z][^A-Z]+|$)")

func fieldNameToEnvName(s string) string {
	var a []string
	for _, sub := range camel.FindAllStringSubmatch(s, -1) {
		if sub[1] != "" {
			a = append(a, sub[1])
		}
		if sub[2] != "" {
			a = append(a, sub[2])
		}
	}
	return strings.ToUpper(strings.Join(a, "_"))
}

// preparations for docker
func handleEnvironmentVars(config *Config) {
	configValue := reflect.Indirect(reflect.ValueOf(config))
	configType := configValue.Type()
	for index := 0; index < configType.NumField(); index++ {
		fieldName := configType.Field(index).Name
		envName := fieldNameToEnvName(fieldName)
		envValue := os.Getenv(envName)
		if envValue != "" {
			fmt.Println("use environment variable: ", envName, " = ", envValue)
			if configValue.FieldByName(fieldName).Kind() == reflect.Int64 {
				i, _ := strconv.ParseInt(envValue, 10, 64)
				configValue.FieldByName(fieldName).SetInt(i)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.String {
				configValue.FieldByName(fieldName).SetString(envValue)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Bool {
				b, _ := strconv.ParseBool(envValue)
				configValue.FieldByName(fieldName).SetBool(b)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Float64 {
				f, _ := strconv.ParseFloat(envValue, 64)
				configValue.FieldByName(fieldName).SetFloat(f)
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Slice {
				val := []string{}
				for _, element := range strings.Split(envValue, ",") {
					val = append(val, strings.TrimSpace(element))
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(val))
			}
			if configValue.FieldByName(fieldName).Kind() == reflect.Map {
				value := map[string]string{}
				for _, element := range strings.Split(envValue, ",") {
					keyVal := strings.Split(element, ":")
					key := strings.TrimSpace(keyVal[0])
					val := strings.TrimSpace(keyVal[1])
					value[key] = val
				}
				configValue.FieldByName(fieldName).Set(reflect.ValueOf(value))
			}
		}
	}
}
