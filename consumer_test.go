package main

import (
	"fmt"
	"testing"
)

var (
	CC = ConsumerController{Run: true, LastError: nil}

	NullMsg_00 = *kafka.Message{}
	SampleMsgKey string = "SampleKey"
	SampleMsgValue string = "SampleValue"
	SampleTopic string = "sample-topic"
    SampleMsg_00 = *kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic: &SampleTopic, 
			Partition: kafka.PartitionAny
		},
		Key:            []byte(SampleMsgKey),
		Value:          []byte(SampleMsgValue),
		Headers:        []kafka.Header{
			{Key: "somekey", Value: []byte(SampleTopic)},
		},
	}
)


type EventHandlerTest struct {
	Descriptor   string
	InputEvent  *kafka.Event
	Expected	 ConsumerController
}

func TestHandleEvent(t *testing.T, event interface{}) {
	Consumer = &kafka.Consumer{}



}