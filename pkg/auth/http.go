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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"runtime/debug"
	"time"
)

var ErrorNotFound = errors.New("not found")
var ErrorAccessDenied = errors.New("access denied")
var ErrorUnexpectedStatus = errors.New("unexpected status")

func (this JwtToken) Post(url string, contentType string, body io.Reader) (resp *http.Response, err error) {
	req, err := http.NewRequest("POST", url, body)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	req.WithContext(ctx)
	req.Header.Set("Authorization", string(this))
	req.Header.Set("Content-Type", contentType)

	resp, err = http.DefaultClient.Do(req)

	if err == nil {
		if resp.StatusCode == http.StatusNotFound {
			//body muss be read til eof and closed to enable connection reuse
			io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, ErrorNotFound
		}
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
			//body muss be read til eof and closed to enable connection reuse
			io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, ErrorAccessDenied
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, err := io.ReadAll(resp.Body)
			if err != nil {
				log.Println("ERROR: ", err)
			}
			if err := resp.Body.Close(); err != nil {
				log.Println("ERROR: ", err)
			}
			log.Println("DEBUG: response:", resp.StatusCode, string(b))
			debug.PrintStack()
			return resp, ErrorUnexpectedStatus
		}
	}
	return
}

func (this JwtToken) PostJSON(url string, body interface{}, result interface{}) (err error) {
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(body)
	if err != nil {
		return
	}
	resp, err := this.Post(url, "application/json", b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if result != nil {
		err = json.NewDecoder(resp.Body).Decode(result)
	}
	return
}

func (this JwtToken) Get(url string) (resp *http.Response, err error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	req.WithContext(ctx)
	req.Header.Set("Authorization", string(this))
	resp, err = http.DefaultClient.Do(req)

	if err == nil {
		if resp.StatusCode == http.StatusNotFound {
			//body muss be read til eof and closed to enable connection reuse
			io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, ErrorNotFound
		}
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
			//body muss be read til eof and closed to enable connection reuse
			io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, ErrorAccessDenied
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Println("ERROR: ", err)
			}
			if err := resp.Body.Close(); err != nil {
				log.Println("ERROR: ", err)
			}
			log.Println("DEBUG: response:", resp.StatusCode, string(b))
			debug.PrintStack()
			return resp, ErrorUnexpectedStatus
		}
	}
	return
}

func (this JwtToken) GetJSON(url string, result interface{}) (err error) {
	resp, err := this.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(result)
}

func (this JwtToken) Delete(url string) (resp *http.Response, err error) {
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	req.WithContext(ctx)
	req.Header.Set("Authorization", string(this))

	resp, err = http.DefaultClient.Do(req)

	if err == nil {
		if resp.StatusCode == http.StatusNotFound {
			//body muss be read til eof and closed to enable connection reuse
			io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, ErrorNotFound
		}
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
			//body muss be read til eof and closed to enable connection reuse
			io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, ErrorAccessDenied
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Println("ERROR: ", err)
			}
			if err := resp.Body.Close(); err != nil {
				log.Println("ERROR: ", err)
			}
			log.Println("DEBUG: response:", resp.StatusCode, string(b))
			debug.PrintStack()
			return resp, ErrorUnexpectedStatus
		}
	}
	return
}

func (this JwtToken) Put(url string, contentType string, body io.Reader) (resp *http.Response, err error) {
	req, err := http.NewRequest("PUT", url, body)
	if err != nil {
		return nil, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	req.WithContext(ctx)
	req.Header.Set("Authorization", string(this))
	req.Header.Set("Content-Type", contentType)

	resp, err = http.DefaultClient.Do(req)

	if err == nil {
		if resp.StatusCode == http.StatusNotFound {
			//body muss be read til eof and closed to enable connection reuse
			io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, ErrorNotFound
		}
		if resp.StatusCode == http.StatusForbidden || resp.StatusCode == http.StatusUnauthorized {
			//body muss be read til eof and closed to enable connection reuse
			io.ReadAll(resp.Body)
			resp.Body.Close()
			return resp, ErrorAccessDenied
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				log.Println("ERROR: ", err)
			}
			if err := resp.Body.Close(); err != nil {
				log.Println("ERROR: ", err)
			}
			log.Println("DEBUG: response:", resp.StatusCode, string(b))
			debug.PrintStack()
			return resp, ErrorUnexpectedStatus
		}
	}
	return
}

func (this JwtToken) PutJSON(url string, body interface{}, result interface{}) (err error) {
	b := new(bytes.Buffer)
	err = json.NewEncoder(b).Encode(body)
	if err != nil {
		return
	}
	resp, err := this.Put(url, "application/json", b)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if result != nil {
		err = json.NewDecoder(resp.Body).Decode(result)
	}
	return
}

func (this JwtToken) Head(url string) (statuscode int, err error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return statuscode, err
	}
	ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
	req.WithContext(ctx)
	req.Header.Set("Authorization", string(this))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return statuscode, err
	}
	defer resp.Body.Close()
	defer io.ReadAll(resp.Body)
	return resp.StatusCode, nil
}
