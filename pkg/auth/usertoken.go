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
	"log"
	"net/http"
	"net/url"
)

func (auth *Auth) GetUserToken(username string, password string) (token JwtToken, err error) {
	openid, err := GetOpenidPasswordToken(auth.authEndpoint, auth.authClientId, auth.authClientSecret, username, password)
	if err == nil && openid.AccessToken != "" {
		auth.saveTokenToCache(username, openid.JwtToken())
	}
	return openid.JwtToken(), err
}

func (auth *Auth) ExchangeUserToken(userid string) (token JwtToken, err error) {
	resp, err := http.PostForm(auth.authEndpoint+"/auth/realms/master/protocol/openid-connect/token", url.Values{
		"client_id":         {auth.authClientId},
		"client_secret":     {auth.authClientSecret},
		"grant_type":        {"urn:ietf:params:oauth:grant-type:token-exchange"},
		"requested_subject": {userid},
	})
	if err != nil {
		return
	}
	if resp.StatusCode != http.StatusOK {
		body, _ := ioutil.ReadAll(resp.Body)
		log.Println("ERROR: GetUserToken()", resp.StatusCode, string(body))
		err = errors.New("access denied")
		resp.Body.Close()
		return
	}
	var openIdToken OpenidToken
	err = json.NewDecoder(resp.Body).Decode(&openIdToken)
	if err != nil {
		return
	}
	return JwtToken("Bearer " + openIdToken.AccessToken), nil
}
