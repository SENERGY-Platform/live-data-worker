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

package cache

import (
	"errors"
	gocache "github.com/patrickmn/go-cache"
	"time"
)

type Cache struct {
	cache *gocache.Cache
}

type Item struct {
	Key   string
	Value []byte
}

var ErrNotFound = errors.New("key not found in cache")

func New() *Cache {

	return &Cache{cache: gocache.New(10*time.Second, 30*time.Second)}
}
func (c *Cache) Get(key string) (item *Item, err error) {
	value, ok := c.cache.Get(key)
	if !ok {
		return nil, errors.New("cache miss")
	}

	item.Value, ok = value.([]byte)
	if !ok {
		return nil, errors.New("value not byte")
	}
	return

}

func (c *Cache) Set(key string, value []byte, expiration int32) {
	c.cache.Set(key, value, time.Duration(expiration)*time.Second)
}

func (c *Cache) Remove(key string) {
	c.cache.Delete(key)
}
