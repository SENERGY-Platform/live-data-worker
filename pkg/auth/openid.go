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
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"time"

	"net/url"
)

type JwtToken string

type OpenidToken struct {
	AccessToken      string    `json:"access_token"`
	ExpiresIn        float64   `json:"expires_in"`
	RefreshExpiresIn float64   `json:"refresh_expires_in"`
	RefreshToken     string    `json:"refresh_token"`
	TokenType        string    `json:"token_type"`
	RequestTime      time.Time `json:"-"`
}

func (this *OpenidToken) JwtToken() JwtToken {
	return JwtToken("Bearer " + this.AccessToken)
}

func GetOpenidPasswordToken(authEndpoint string, authClientId string, authClientSecret string, username, password string) (token OpenidToken, err error) {
	requesttime := time.Now()
	client := http.Client{
		Timeout: 5 * time.Second,
	}
	resp, err := client.PostForm(authEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":     {authClientId},
		"client_secret": {authClientSecret},
		"username":      {username},
		"password":      {password},
		"grant_type":    {"password"},
	})

	if err != nil {
		return token, err
	}
	if resp.StatusCode >= 300 {
		b, _ := ioutil.ReadAll(resp.Body)
		err = errors.New(resp.Status + ": " + string(b))
		return
	}
	err = json.NewDecoder(resp.Body).Decode(&token)
	token.RequestTime = requesttime
	return
}
