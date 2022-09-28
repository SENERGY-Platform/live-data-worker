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

package util

import (
	"log"
	"net/http"
	"os"
)

var VersionTxtFile = "version.txt"

func NewVersionHeaderMiddleware(handler http.Handler) http.Handler {
	version, err := os.ReadFile(VersionTxtFile)
	if err != nil {
		log.Println("unable to find version.txt")
		return handler
	}
	return &VersionHeaderMiddleware{handler: handler, version: string(version)}
}

type VersionHeaderMiddleware struct {
	handler http.Handler
	version string
}

func (this *VersionHeaderMiddleware) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if this.handler != nil {
		w.Header().Set("X-SENERGY-SERVICE-VERSION", this.version)
		this.handler.ServeHTTP(w, r)
	} else {
		http.Error(w, "Forbidden", 403)
	}
}
