package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"kafka-clone/internal/broker"
)

func main() {
	var (
		topic      = flag.String("topic", "default", "Topic name")
		key        = flag.String("key", "", "Message key")
		value      = flag.String("value", "", "Message value")
		consume    = flag.Bool("consume", false, "Consume messages from a topic")
		group      = flag.String("group", "default-group", "Consumer group label")
		offset     = flag.Int("offset", 0, "Offset to begin consuming from")
		listTopics = flag.Bool("list-topics", false, "List all existing topics")
		demo       = flag.Bool("demo", false, "Run a short demonstration")
		dataDir    = flag.String("data-dir", "./data", "Directory for topic logs")
	)
	flag.Parse()

	b, err := broker.NewBroker(*dataDir)
	if err != nil {
		fatal(err)
	}

	if *demo {
		runDemo(b)
		return
	}

	if *listTopics {
		topics, err := b.Topics()
		if err != nil {
			fatal(err)
		}
		fmt.Println(strings.Join(topics, ", "))
		return
	}

	if *consume {
		messages, err := b.Consume(*topic, *offset)
		if err != nil {
			fatal(err)
		}
		for _, msg := range messages {
			fmt.Printf("group=%s offset=%d key=%s value=%s ts=%s\n", *group, msg.ID, msg.Key, msg.Value, msg.Timestamp.Format(time.RFC3339))
		}
		return
	}

	if *value == "" {
		fmt.Println("value is required unless --consume or --demo is used")
		os.Exit(1)
	}

	msg, err := b.Publish(*topic, *key, *value)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("published topic=%s key=%s value=%s id=%d\n", msg.Topic, msg.Key, msg.Value, msg.ID)
}

func runDemo(b *broker.Broker) {
	_, _ = b.Publish("orders", "user-1", "created")
	_, _ = b.Publish("orders", "user-1", "paid")
	_, _ = b.Publish("payments", "charge-1", "approved")

	fmt.Println("Demo topic messages:")
	orders, err := b.Consume("orders", 0)
	if err != nil {
		fatal(err)
	}
	for _, msg := range orders {
		fmt.Printf("topic=%s key=%s value=%s\n", msg.Topic, msg.Key, msg.Value)
	}
	fmt.Println("Demo complete")
}

func fatal(err error) {
	fmt.Println("error:", err)
	os.Exit(1)
}
