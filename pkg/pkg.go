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
	"github.com/SENERGY-Platform/live-data-worker/pkg/taskmanager"
	"log"
)

func Start(ctx context.Context, config configuration.Config) error {
	onTaskChanged := func(map[string]taskmanager.Task) {
		log.Println("task list changed, but task handling not implemented")
	}

	if !config.MgwMode {
		// TODO overwrite onTaskChanged: consume from Kafka
	} else {
		// TODO overwrite onTaskChanged: consume from MQTT
	}

	manager := taskmanager.New(onTaskChanged)
	if !config.MgwMode {
		authentication := auth.New(config.AuthEndpoint, config.AuthClientId, config.AuthClientSecret)
		err := api.Start(ctx, config, manager, authentication)
		if err != nil {
			return err
		}
	} else {
		// TODO consume tasks from MQTT
	}

	return nil
}
