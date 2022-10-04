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

package auth

import (
	"github.com/SENERGY-Platform/live-data-worker/pkg/cache"
	"sync"
)

func New(authEndpoint, authClientId, authClientSecret, authUsername, authPassword string) *Auth {
	return &Auth{
		authEndpoint:         authEndpoint,
		authClientSecret:     authClientSecret,
		authClientId:         authClientId,
		authUsername:         authUsername,
		authPassword:         authPassword,
		tokenCacheExpiration: 3600,
		cache:                cache.New(),
	}
}

type Auth struct {
	authEndpoint         string
	authClientId         string
	authClientSecret     string
	authUsername         string
	authPassword         string
	openid               *OpenidToken
	mux                  sync.Mutex
	cache                *cache.Cache
	tokenCacheExpiration int32
}
