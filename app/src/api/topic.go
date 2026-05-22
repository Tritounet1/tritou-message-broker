package api

import (
	"path/filepath"
	"sync"
	tp "tidy/topic"
	"time"
)

// topic représente un topic et ses messages stockés en mémoire.
type topic struct {
	mu          sync.RWMutex
	name        string
	log         *Log
	subscribers map[string]chan *tp.Message
}

func newTopic(name string, dataDir string, maxSegSize int64) (*topic, error) {
	dir := filepath.Join(dataDir, name)
	l, err := openLog(dir, maxSegSize)
	if err != nil {
		return nil, err
	}
	return &topic{
		name:        name,
		log:         l,
		subscribers: make(map[string]chan *tp.Message),
	}, nil
}

func (t *topic) addSubscriber(id string) chan *tp.Message {
	t.mu.Lock()
	defer t.mu.Unlock()
	ch := make(chan *tp.Message, 64)
	t.subscribers[id] = ch
	return ch
}

func (t *topic) removeSubscriber(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if ch, ok := t.subscribers[id]; ok {
		close(ch)
		delete(t.subscribers, id)
	}
}

func (t *topic) append(msg *tp.Message) (int64, error) {
	msg.TimestampMs = time.Now().UnixMilli()
	msg.Topic = t.name

	// L'append au log est protégé par son propre mutex,
	// pas besoin de prendre t.mu pour ça.
	offset, err := t.log.Append(msg)
	if err != nil {
		return 0, err
	}

	// Fanout aux subscribers
	t.mu.RLock()
	subs := make([]chan *tp.Message, 0, len(t.subscribers))
	for _, ch := range t.subscribers {
		subs = append(subs, ch)
	}
	t.mu.RUnlock()

	for _, ch := range subs {
		select {
		case ch <- msg:
		default:
		}
	}
	return offset, nil
}

func (t *topic) snapshotFrom(offset int64) ([]*tp.Message, error) {
	return t.log.ReadFrom(offset)
}
