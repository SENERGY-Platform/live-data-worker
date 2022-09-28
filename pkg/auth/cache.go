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
	"log"
)

func (auth *Auth) GetCachedUserToken(username string) (token JwtToken, err error) {
	if auth.cache != nil {
		token, err = auth.getTokenFromCache(username)
		if err == nil {
			return
		}
		if err != cache.ErrNotFound {
			log.Println("ERROR: GetCachedUserToken() ", err)
		}
	}
	token, err = auth.ExchangeUserToken(username)
	if err != nil {
		log.Println("ERROR: GetCachedUserToken::GenerateUserToken()", err, username)
		return
	}
	if auth.cache != nil {
		auth.saveTokenToCache(username, token)
	}
	return
}

func (auth *Auth) getTokenFromCache(username string) (token JwtToken, err error) {
	item, err := auth.cache.Get("token." + username)
	if err != nil {
		return token, err
	}
	return JwtToken(item.Value), err
}

func (auth *Auth) saveTokenToCache(username string, token JwtToken) {
	auth.cache.Set("token."+username, []byte(token), auth.tokenCacheExpiration)
}
