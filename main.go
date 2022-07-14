package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
	"time"
	"math/rand"
	"encoding/json"
	log "github.com/sirupsen/logrus"
	"io/ioutil"
	"github.com/instantpun/kafka-consumer/utils"
)

// setup RetryController
var BackoffCtl utils.RetryConfig

type ConsumerController struct {
	Run bool
}

func handleEvent(cc *ConsumerController, consumer *kafka.Consumer, event interface{}) error {

	switch evt := event.(type) {
	case *kafka.Message:
		// process message
		fmt.Printf("%% Message on %s: %s", evt.TopicPartition, string(evt.Value))
		if evt.Headers != nil {
			fmt.Printf("%% Headers: %v", evt.Headers)
		}

		if BackoffCtl.DelayCounter > 1 {
			BackoffCtl.ResetDelay()
		}
		return nil
	case kafka.Error:
		// Errors should generally be considered
		// informational, the client will try to 
		// automatically recover.
		// But we choose to terminate the application
		// if all brokers are down
		
		switch errCode := evt.Code(); errCode {
		case kafka.ErrAllBrokersDown:
			cc.Run = false
			return errCode
		default:
			fmt.Printf("Recieved unhandle, d error: %v", errCode)
			return errCode // WARN: this may not work, is return value from kafka.Error.Code() of type error?
		}
	case kafka.PartitionEOF:
		fmt.Printf("Reached end of message log %v\n", evt)
		return nil
	case nil:
		fmt.Printf("No new events received %v\n", evt)
		// sleep, then increase retry delay
		BackoffCtl.Wait()
		BackoffCtl.IncDelay()
		return nil
	case kafka.OffsetsCommitted:
		fallthrough
	case kafka.AssignedPartitions:
		fallthrough
	case kafka.RevokedPartitions:
		fallthrough
	case kafka.OAuthBearerTokenRefresh:
		fallthrough
	default:
		fmt.Printf("Ignored %v", evt)
		return nil
	}
}


func main() {

	initLoggerDefaults()
	
	RootLogFields["program"] = "go-consumer"
	RootLogger := log.WithFields(RootLogFields)
	// testLogger.Info("test")

	BackoffCtl, _ = (*utils.NewRetryConfig("exponential"))

	if len(os.Args) < 3 {
		host, _ := os.Hostname()
		RootLogger.WithFields(log.Fields{"host": host}).Fatalf("Usage: %s broker group topics...",
		os.Args[0])
	}

	cfgFile := os.Args[1]
	broker := os.Args[2]
	groupId := os.Args[3]
	topics := os.Args[4:]

	instanceId := os.Getenv("GROUP_INSTANCE_ID")
	if instanceId == "" {
		instanceId, _ = os.Hostname()
	}

	RootLogger = RootLogger.WithFields(log.Fields{
		"group_id": groupId,
		"instance_id": instanceId,
		"topics": fmt.Sprintf("%v", topics), // not sure how to change into normal slice/list
	})
	RootLogger.Info("test")

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM) // listen for process signals and add to channel

	consumerConfig, err := loadConfig(cfgFile)
	log.Infof("%v", consumerConfig)
	// consumerConfig["bootstrap.servers"] = broker
	// consumerConfig["group.id"] = groupId
	// consumerConfig["group.instance.id"] = instanceId
	
	consumer, err := kafka.NewConsumer()


	// consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
	// 	"bootstrap.servers": broker,
	// 	"broker.address.family":           "v4",
	// 	"group.id":                        groupId,
	// 	"group.instance.id":               instanceId,
	// 	"session.timeout.ms":              6000,
	// 	"go.events.channel.enable":        false,
	// 	"go.application.rebalance.enable": true,
	// 	"enable.partition.eof":            true,
	// 	"auto.offset.reset":               "latest"})

	if err != nil {
		rootLogger.WithFields(log.Fields{"error": fmt.Sprintf("%v", err)}).Fatal("Failed to create consumer\n")
	}

	rootLogger.WithFields(log.Fields{"consumer_name": fmt.Sprintf("%v", consumer)}).Info("Created Consumer\n")

	err = consumer.SubscribeTopics(topics, nil)

	defer func() {
		fmt.Printf("Closing consumer\n")
		consumer.Close()
	}()

	cc := &ConsumerController{Run: true}

	for cc.Run {
		rootLogger.Info("Processing event stream...")
		select {
		case sig := <-sigchan:
			s := fmt.Sprintf("%v", sig)
			log.WithFields(log.Fields{"signal": s}).Warn("Caught termination signal. Terminating...") 
			cc.Run = false
		default:
			rootLogger.Info("Polling for latest events...")
			event := consumer.Poll(100)

			handleEvent(cc, consumer, event)
		}
	}

}


func proccessMessage(msg *kafka.Message) {

	msgLogFields := log.Fields{
		"topic":     msg.TopicPartition.Topic,
		"partition": msg.TopicPartition.Partition,
		"offset":    msg.TopicPartition.Offset,
		"key":       msg.Key,
		"value":     string(msg.Value),
		"message_timestamp": fmt.Sprintf("%v", msg.Timestamp),
	}
	
	rootLogger := rootLogger.WithFields(msgLogFields)
	
	if msg.Headers != nil {
		headers := fmt.Sprintf("%v", msg.Headers)
		rootLogger.WithFields(log.Fields{"headers": headers}).Info("Message received")
	} else {
		rootLogger.Info("Message received")
	}
}