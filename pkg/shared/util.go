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

package shared

import (
	"errors"
	"github.com/SENERGY-Platform/live-data-worker/pkg/auth"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/iot"
	"github.com/SENERGY-Platform/live-data-worker/pkg/taskmanager"
	"strconv"
	"strings"
	"time"
)

func ParseSubscriptionTopic(config configuration.Config, topic string, username string, authentication *auth.Auth) (deviceId, localDeviceId,
	serviceId, localServiceId, functionId, aspectId, characteristicId string, err error) {

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

	deviceId, serviceId, functionId, aspectId, characteristicId = topicParts[0], topicParts[1], topicParts[2], topicParts[3], topicParts[4]
	meta, err := iot.SingletonOrErr()
	if err != nil {
		return deviceId, localDeviceId,
			serviceId, localServiceId, functionId, aspectId, characteristicId, err
	}
	var jwt auth.JwtToken
	if config.MgwMode {
		jwt, err = authentication.GetSelfToken()

	} else {
		jwt, err = authentication.GetCachedUserToken(username)
	}
	if err != nil {
		return deviceId, localDeviceId,
			serviceId, localServiceId, functionId, aspectId, characteristicId, err
	}
	token := string(jwt)

	device, err := meta.GetDevice(token, deviceId)
	if err != nil {
		return deviceId, localDeviceId,
			serviceId, localServiceId, functionId, aspectId, characteristicId, err
	}
	localDeviceId = device.LocalId

	service, err := meta.GetService(token, device, serviceId)
	if err != nil {
		return deviceId, localDeviceId,
			serviceId, localServiceId, functionId, aspectId, characteristicId, err
	}
	localServiceId = service.LocalId

	/* TODO
	if !config.MgwMode {
		token, err := authentication.GetCachedUserToken(username)
		if err != nil {
			return deviceId, localDeviceId,
				serviceId, localServiceId, functionId, aspectId, characteristicId, err
		}
		resp, err := token.Get(config.PermissionsUrl + "/v3/resources/devices?rights=x&filter=local_id=" + localDeviceId)
		if err != nil {
			return deviceId, localDeviceId,
				serviceId, localServiceId, functionId, aspectId, characteristicId, err
		}
		if resp.StatusCode > 299 {
			err = errors.New("unexpected statuscode " + strconv.Itoa(resp.StatusCode))
			return deviceId, localDeviceId,
				serviceId, localServiceId, functionId, aspectId, characteristicId, err
		}
		var devices []Device
		err = json.NewDecoder(resp.Body).Decode(&devices)
		if err != nil {
			return deviceId, localDeviceId,
				serviceId, localServiceId, functionId, aspectId, characteristicId, err
		}
		if len(devices) != 1 {
			err = errors.New("unexpected number of devices received from permSearch")
			return deviceId, localDeviceId,
				serviceId, localServiceId, functionId, aspectId, characteristicId, err
		}
		deviceId = devices[0].Id
	}
	*/
	return
}

func GetTaskId(clientId string, topic string) string {
	return clientId + "_" + topic
}

func Apsert(m map[string][]taskmanager.Task, key string, task taskmanager.Task) map[string][]taskmanager.Task {
	arr, ok := m[key]
	if !ok {
		arr = []taskmanager.Task{}
	}
	arr = append(arr, task)
	m[key] = arr
	return m
}

func ElemInSlice(elem string, slice []string) bool {
	for _, sliceElem := range slice {
		if elem == sliceElem {
			return true
		}
	}
	return false
}

func EqualStringSliceIgnoreOrder(a []string, b []string) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if len(a) != len(b) {
		return false
	}
	for _, aElem := range a {
		if !ElemInSlice(aElem, b) {
			return false
		}
	}
	return true
}

func GetMissingOrAddedElements(base []string, update []string) (missing []string, added []string) {
	missing, added = []string{}, []string{}
	if base == nil {
		return missing, update
	}
	if update == nil {
		return base, added
	}
	for _, baseElem := range base {
		if !ElemInSlice(baseElem, update) {
			missing = append(missing, baseElem)
		}
	}
	for _, updateElem := range update {
		if !ElemInSlice(updateElem, base) {
			added = append(added, updateElem)
		}
	}
	return
}

func GetOutputMqttTopic(config configuration.Config, task taskmanager.Task) string {
	return config.MqttPrefix + task.Info.DeviceId + "/" + task.Info.ServiceId + "/" + task.Info.FunctionId + "/" +
		task.Info.AspectId + "/" + task.Info.CharacteristicId
}

func GetLocalTime() string {
	t := time.Now()
	return strconv.Itoa(t.Year()) + "-" + t.Month().String() + "-" + strconv.Itoa(t.Day()) + "T" +
		strconv.Itoa(t.Hour()) + ":" + strconv.Itoa(t.Minute()) + ":" + strconv.Itoa(t.Second())
}
