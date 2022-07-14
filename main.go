package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"os"
	"os/signal"
	"syscall"
	// "time"
	// "math/rand"
	// "encoding/json"
	// "io/ioutil"
	log "github.com/sirupsen/logrus"
	"github.com/instantpun/kafka-consumer/utils"
)

type RuntimeController struct {
	Run 	 bool
	RetryCtl *utils.RetryConfig
}

func handleEvent(rc *RuntimeController, consumer *kafka.Consumer, event interface{}) error {

	switch evt := event.(type) {
	case *kafka.Message:
		fmt.Printf("%% Message on %s: %s", evt.TopicPartition, string(evt.Value))
		if evt.Headers != nil {
			fmt.Printf("%% Headers: %v", evt.Headers)
		}

		proccessMessage(evt)
		if rc.RetryCtl.DelayCounter > 1 {
			rc.RetryCtl.ResetDelay()
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
			rc.Run = false
			return evt
		default:
			fmt.Printf("Recieved unhandle, d error: %v", errCode)
			return evt // WARN: this may not work, is return value from kafka.Error.Code() of type error?
		}
	case kafka.PartitionEOF:
		fmt.Printf("Reached end of message log %v\n", evt)
		return nil
	case nil:
		fmt.Printf("No new events received %v\n", evt)
		// sleep, then increase retry delay
		rc.RetryCtl.Wait()
		rc.RetryCtl.IncDelay()
		return nil
	case kafka.OffsetsCommitted:
		fmt.Printf("Ignored %v", evt)
		return nil
	case kafka.AssignedPartitions:
		fmt.Printf("Ignored %v", evt)
		return nil
	case kafka.RevokedPartitions:
		fmt.Printf("Ignored %v", evt)
		return nil
	case kafka.OAuthBearerTokenRefresh:
		fmt.Printf("Ignored %v", evt)
		return nil
	default:
		fmt.Printf("Ignored %v", evt)
		return nil
	}
}


func main() {

	initLoggerDefaults()
	
	RootLogFields["program"] = "go-consumer"
	RootLogger := log.WithFields(RootLogFields)

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

	// consumerConfig, err := loadConfig(cfgFile)
	// log.Infof("%v", consumerConfig)
	// consumerConfig["bootstrap.servers"] = broker
	// consumerConfig["group.id"] = groupId
	// consumerConfig["group.instance.id"] = instanceId
	
	// consumer, err := kafka.NewConsumer()


	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"broker.address.family":           "v4",
		"group.id":                        groupId,
		"group.instance.id":               instanceId,
		"session.timeout.ms":              6000,
		"go.events.channel.enable":        false,
		"go.application.rebalance.enable": true,
		"enable.partition.eof":            true,
		"auto.offset.reset":               "latest"})

	if err != nil {
		RootLogger.WithFields(log.Fields{"error": fmt.Sprintf("%v", err)}).Fatal("Failed to create consumer\n")
	}

	RootLogger.WithFields(log.Fields{"consumer_name": fmt.Sprintf("%v", consumer)}).Info("Created Consumer\n")

	err = consumer.SubscribeTopics(topics, nil)

	defer func() {
		fmt.Printf("Closing consumer\n")
		consumer.Close()
	}()

	tmp, _ := utils.NewRetryConfig("exponential")
	rc := &RuntimeController{
		Run: true,
		RetryCtl: tmp,
	}

	for rc.Run {
		RootLogger.Info("Processing event stream...")
		select {
		case sig := <-sigchan:
			s := fmt.Sprintf("%v", sig)
			log.WithFields(log.Fields{"signal": s}).Warn("Caught termination signal. Terminating...") 
			rc.Run = false
		default:
			RootLogger.Info("Polling for latest events...")
			event := consumer.Poll(100)

			handleEvent(rc, consumer, event)
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
	
	RootLogger := RootLogger.WithFields(msgLogFields)
	
	if msg.Headers != nil {
		headers := fmt.Sprintf("%v", msg.Headers)
		RootLogger.WithFields(log.Fields{"headers": headers}).Info("Message received")
	} else {
		RootLogger.Info("Message received")
	}
}