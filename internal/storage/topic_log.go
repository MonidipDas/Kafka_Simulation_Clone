package storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"kafka-clone/internal/models"
)

type TopicLog struct {
	name string
	path string
	file *os.File
	mu   sync.Mutex
}

func NewTopicLog(dir, topic string) (*TopicLog, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}

	path := filepath.Join(dir, topic+".log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	return &TopicLog{
		name: topic,
		path: path,
		file: file,
	}, nil
}

func (t *TopicLog) Append(msg models.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	if _, err := t.file.Write(append(data, '\n')); err != nil {
		return fmt.Errorf("write message: %w", err)
	}

	return t.file.Sync()
}

func (t *TopicLog) ReadAll() ([]models.Message, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, err := t.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("seek log: %w", err)
	}

	scanner := bufio.NewScanner(t.file)
	messages := make([]models.Message, 0)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		var message models.Message
		if err := json.Unmarshal([]byte(line), &message); err != nil {
			return nil, fmt.Errorf("decode message in %s: %w", t.name, err)
		}
		messages = append(messages, message)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read log %s: %w", t.name, err)
	}

	if _, err := t.file.Seek(0, 2); err != nil {
		return nil, fmt.Errorf("reset log cursor: %w", err)
	}

	return messages, nil
}
