package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

var phoneRegex = regexp.MustCompile(`(?i)\b(?:\(?\d{3}\)?[-.\s]*)?\d{3}[-.\s]?\d{4}\b`)

func extractPhoneNumbers(text string) []string {
	return phoneRegex.FindAllString(text, -1)
}

func main() {
	brokers := []string{"localhost:9092"}
	topic := "test-topic"

	// Open the input file
	file, err := os.Open("reader/input.txt")
	if err != nil {
		log.Fatalf("Failed to open file: %v", err)
	}
	defer file.Close()

	// Set up async Kafka producer
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		log.Fatalf("Failed to start Kafka async producer: %v", err)
	}
	defer func() {
		if err := producer.Close(); err != nil {
			log.Printf("Error closing producer: %v", err)
		}
	}()

	// Goroutine to log successful sends
	go func() {
		for msg := range producer.Successes() {
			fmt.Printf("✅ Message sent to partition %d at offset %d: %s\n",
				msg.Partition, msg.Offset, msg.Value)
		}
	}()

	// Goroutine to log failed sends
	go func() {
		for err := range producer.Errors() {
			log.Printf("❌ Failed to send message: %v", err.Err)
		}
	}()

	scanner := bufio.NewScanner(file)
	var prevLine string

	for scanner.Scan() {
		currLine := strings.TrimSpace(scanner.Text())
		if currLine == "" {
			prevLine = ""
			continue
		}

		combined := prevLine + " " + currLine
		numbers := extractPhoneNumbers(combined)

		for _, number := range numbers {
			msg := &sarama.ProducerMessage{
				Topic: topic,
				Value: sarama.StringEncoder(number),
			}
			producer.Input() <- msg // Send asynchronously
		}

		prevLine = currLine
	}

	if err := scanner.Err(); err != nil {
		log.Fatalf("Error reading file: %v", err)
	}

	// Give producer time to flush messages
	time.Sleep(2 * time.Second)
}
