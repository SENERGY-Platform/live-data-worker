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

package marshaller

import (
	"context"
	"encoding/json"
	"github.com/SENERGY-Platform/device-command/pkg/command/dependencies/impl/cloud"
	"github.com/SENERGY-Platform/device-command/pkg/command/dependencies/impl/mgw"
	"github.com/SENERGY-Platform/device-command/pkg/command/dependencies/interfaces"
	"github.com/SENERGY-Platform/external-task-worker/lib/devicerepository/model"
	marshlib "github.com/SENERGY-Platform/external-task-worker/lib/marshaller"
	"github.com/SENERGY-Platform/live-data-worker/pkg/auth"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/iot"
)

type Marshaller struct {
	marsh          interfaces.Marshaller
	iot            interfaces.Iot
	authentication *auth.Auth
	config         configuration.Config
}

func NewMarshaller(ctx context.Context, config configuration.Config, authentication *auth.Auth) (marshaller *Marshaller, err error) {
	dcConf := config.ToDcConf()
	var marsh interfaces.Marshaller
	meta, err := iot.NewIoT(ctx, config)
	if err != nil {
		return
	}
	if !config.MgwMode {
		marsh, err = cloud.MarshallerFactory(ctx, dcConf, meta)
		if err != nil {
			return
		}
	} else {
		marsh, err = mgw.MarshallerFactory(ctx, dcConf, meta)
		if err != nil {
			return
		}
	}

	return &Marshaller{
		marsh:          marsh,
		iot:            meta,
		authentication: authentication,
		config:         config,
	}, err
}

func (m *Marshaller) Resolve(deviceId, aspectId, functionId, serviceId, characteristicId, username string, msg interface{}) (value interface{}, err error) {
	var service model.Service
	var protocol model.Protocol
	var aspectNode model.AspectNode
	var jwt auth.JwtToken
	if m.config.MgwMode {
		jwt, err = m.authentication.GetSelfToken()
	} else {
		jwt, err = m.authentication.GetCachedUserToken(username)
	}
	if err != nil {
		return nil, err
	}

	token := string(jwt)

	device, err := m.iot.GetDevice(token, deviceId)
	if err != nil {
		return nil, err
	}
	service, err = m.iot.GetService(token, device, serviceId)
	if err != nil {
		return nil, err
	}
	protocol, err = m.iot.GetProtocol(token, service.ProtocolId)
	if err != nil {
		return nil, err
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	return m.marsh.UnmarshalV2(marshlib.UnmarshallingV2Request{
		Service:          service,
		Protocol:         protocol,
		CharacteristicId: characteristicId,
		Message: map[string]string{
			"data":     string(b),
			"metadata": "",
		},
		FunctionId:   functionId,
		AspectNode:   aspectNode,
		AspectNodeId: aspectId,
	})
}
