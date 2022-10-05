/*
 * Copyright 2019 InfAI (CC SES)
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

package kafka

import (
	"context"
	"github.com/Shopify/sarama"
	"log"
	"strings"
	"sync"
	"time"
)

const Latest = sarama.OffsetNewest
const Earliest = sarama.OffsetOldest

func newConsumer(ctx context.Context, wg *sync.WaitGroup, kafkaBootstrap string, topics []string, groupId string, offset int64, listener func(topic string, msg []byte, time time.Time) error, errorhandler func(err error, consumer *Consumer), debug bool) (consumer *Consumer, err error) {
	consumer = &Consumer{ctx: ctx, wg: wg, kafkaBootstrap: kafkaBootstrap, topics: topics, listener: listener, errorhandler: errorhandler, offset: offset, ready: make(chan bool), groupId: groupId, debug: debug}
	err = consumer.start()
	return
}

type Consumer struct {
	count          int
	kafkaBootstrap string
	topics         []string
	ctx            context.Context
	wg             *sync.WaitGroup
	listener       func(topic string, msg []byte, time time.Time) error
	errorhandler   func(err error, consumer *Consumer)
	mux            sync.Mutex
	offset         int64
	groupId        string
	ready          chan bool
	debug          bool
}

func (c *Consumer) start() (err error) {
	config := sarama.NewConfig()
	config.Consumer.Offsets.Initial = c.offset

	client, err := sarama.NewConsumerGroup(strings.Split(c.kafkaBootstrap, ","), c.groupId, config)
	if err != nil {
		return err
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				log.Println("close kafka reader")
				return
			default:
				if err = client.Consume(c.ctx, c.topics, c); err != nil {
					c.errorhandler(err, nil)
				}
				// check if context was cancelled, signaling that the Consumer should stop
				if c.ctx.Err() != nil {
					return
				}
				c.ready = make(chan bool)
			}
		}
	}()

	<-c.ready // Await till the Consumer has been set up
	log.Println("Kafka Consumer up and running...")

	return err
}

func (c *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the Consumer as ready
	close(c.ready)
	c.wg.Add(1)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (c *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	log.Println("Cleaned up kafka session")
	c.wg.Done()
	return nil
}

// ConsumeClaim must start a Consumer loop of ConsumerGroupClaim's Messages().
func (c *Consumer) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		select {
		case <-c.ctx.Done():
			log.Println("Ignoring queued kafka messages for faster shutdown")
			return nil
		default:
			if c.debug {
				log.Println(message.Topic, message.Timestamp, string(message.Value))
			}
			err := c.listener(message.Topic, message.Value, message.Timestamp)
			if err != nil {
				c.errorhandler(err, c)
			}
			session.MarkMessage(message, "")
		}
	}

	return nil
}
