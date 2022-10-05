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

package pkg

import (
	"context"
	"github.com/SENERGY-Platform/live-data-worker/pkg/api"
	"github.com/SENERGY-Platform/live-data-worker/pkg/auth"
	"github.com/SENERGY-Platform/live-data-worker/pkg/configuration"
	"github.com/SENERGY-Platform/live-data-worker/pkg/interfaces"
	"github.com/SENERGY-Platform/live-data-worker/pkg/kafka"
	"github.com/SENERGY-Platform/live-data-worker/pkg/mqtt"
	"github.com/SENERGY-Platform/live-data-worker/pkg/taskmanager"
	"sync"
)

func Start(ctx context.Context, onError func(err error), config configuration.Config) (wg *sync.WaitGroup, err error) {
	wg = &sync.WaitGroup{}

	authentication := auth.New(config.AuthEndpoint, config.AuthClientId, config.AuthClientSecret, config.AuthUserName, config.AuthPassword)
	taskManager := taskmanager.New()
	mqttManager, err := mqtt.NewManager(ctx, wg, config, taskManager, authentication)

	var taskHandler interfaces.TaskHandler
	if !config.MgwMode {
		err = api.Start(ctx, config, authentication, mqttManager.Client)
		if err != nil {
			return wg, err
		}
		taskHandler, err = kafka.NewTaskHandler(ctx, config, mqttManager.Client, authentication)
		if err != nil {
			return wg, err
		}
	} else {
		taskHandler, err = mqtt.NewTaskHandler(ctx, mqttManager.Client, config, authentication)
		if err != nil {
			return wg, err
		}
	}
	taskHandler.SetErrorHandler(onError)
	taskManager.SetOnTaskListChanged(taskHandler.UpdateTasks)

	return wg, nil
}
