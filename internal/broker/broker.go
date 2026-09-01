package broker

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"kafka-clone/internal/models"
	"kafka-clone/internal/storage"
)

type Broker struct {
	mu      sync.RWMutex
	dataDir string
	topics  map[string]*storage.TopicLog
}

func NewBroker(dataDir string) (*Broker, error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return nil, fmt.Errorf("create data dir: %w", err)
	}

	return &Broker{
		dataDir: dataDir,
		topics:  make(map[string]*storage.TopicLog),
	}, nil
}

func (b *Broker) EnsureTopic(name string) (*storage.TopicLog, error) {
	b.mu.RLock()
	if log, ok := b.topics[name]; ok {
		b.mu.RUnlock()
		return log, nil
	}
	b.mu.RUnlock()

	b.mu.Lock()
	defer b.mu.Unlock()

	if log, ok := b.topics[name]; ok {
		return log, nil
	}

	log, err := storage.NewTopicLog(b.dataDir, name)
	if err != nil {
		return nil, fmt.Errorf("create topic log: %w", err)
	}

	b.topics[name] = log
	return log, nil
}

func (b *Broker) Publish(topic, key, value string) (models.Message, error) {
	log, err := b.EnsureTopic(topic)
	if err != nil {
		return models.Message{}, err
	}

	msg := models.Message{
		ID:        time.Now().UnixNano(),
		Topic:     topic,
		Key:       key,
		Value:     value,
		Timestamp: time.Now().UTC(),
	}

	if err := log.Append(msg); err != nil {
		return models.Message{}, fmt.Errorf("append to topic %s: %w", topic, err)
	}

	return msg, nil
}

func (b *Broker) Consume(topic string, offset int) ([]models.Message, error) {
	log, err := b.EnsureTopic(topic)
	if err != nil {
		return nil, err
	}

	messages, err := log.ReadAll()
	if err != nil {
		return nil, err
	}

	if offset < 0 {
		offset = 0
	}
	if offset >= len(messages) {
		return nil, nil
	}

	return messages[offset:], nil
}

func (b *Broker) Topics() ([]string, error) {
	entries, err := os.ReadDir(b.dataDir)
	if err != nil {
		return nil, fmt.Errorf("read data dir: %w", err)
	}

	topics := make([]string, 0)
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if strings.HasSuffix(entry.Name(), ".log") {
			topics = append(topics, strings.TrimSuffix(entry.Name(), ".log"))
		}
	}

	sort.Strings(topics)
	return topics, nil
}
