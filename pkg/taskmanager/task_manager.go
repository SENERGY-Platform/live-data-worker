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

package taskmanager

import (
	"golang.org/x/exp/maps"
	"log"
	"strings"
	"sync"
)

type Task struct {
	Id   string
	Info TaskInfo
}

type Manager struct {
	tasks             map[string]Task
	mux               sync.Mutex
	onTaskListChanged func([]Task)
}

func New() *Manager {
	onTaskChanged := func(tasks []Task) {
		log.Println("WARN: task list changed, but task handling not set")
	}
	return &Manager{
		tasks:             make(map[string]Task),
		mux:               sync.Mutex{},
		onTaskListChanged: onTaskChanged,
	}
}

func (manager *Manager) SetOnTaskListChanged(onTaskListChanged func([]Task)) {
	manager.mux.Lock()
	manager.onTaskListChanged = onTaskListChanged
	manager.mux.Unlock()
}

// AddTasks Adds multiple tasks. If a task with the same ID already exists, it is overwritten. Blocks other operations until onTaskListChanged completes.
func (manager *Manager) AddTasks(tasks []Task) {
	manager.mux.Lock()
	for _, task := range tasks {
		manager.tasks[task.Id] = task
	}
	manager.onTaskListChanged(maps.Values(manager.tasks))
	manager.mux.Unlock()
}

// DeleteTasks Deletes multiple task. Blocks other operations until onTaskListChanged completes.
func (manager *Manager) DeleteTasks(ids []string) {
	manager.mux.Lock()
	for _, id := range ids {
		delete(manager.tasks, id)
	}
	manager.onTaskListChanged(maps.Values(manager.tasks))
	manager.mux.Unlock()
}

// AddTask Adds a task. If a task with the same ID already exists, it is overwritten. Blocks other operations until onTaskListChanged completes.
func (manager *Manager) AddTask(task Task) {
	manager.AddTasks([]Task{task})
}

// DeleteTask Deletes a task. Blocks other operations until onTaskListChanged completes.
func (manager *Manager) DeleteTask(id string) {
	manager.DeleteTasks([]string{id})
}

// DeleteTasksWithIdPrefix Deletes all task with an ID that starts with idPrefix. Blocks other operations until onTaskListChanged completes.
func (manager *Manager) DeleteTasksWithIdPrefix(idPrefix string) {
	ids := []string{}
	for k := range manager.tasks {
		if strings.HasPrefix(k, idPrefix) {
			ids = append(ids, k)
		}
	}
	manager.DeleteTasks(ids)
}
