package api

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	tp "tidy/topic"
)

type brokerServer struct {
	tp.UnimplementedBrokerServer

	dataDir    string
	maxSegSize int64

	mu     sync.RWMutex
	topics map[string]*topic

	commitMu sync.RWMutex
	commits  map[string]map[string]int64
}

func newBrokerServer(dataDir string, maxSegSize int64) (*brokerServer, error) {
	b := &brokerServer{
		dataDir:    dataDir,
		maxSegSize: maxSegSize,
		topics:     make(map[string]*topic),
		commits:    make(map[string]map[string]int64),
	}

	// Recharger les topics existants depuis dataDir
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, err
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		t, err := newTopic(name, dataDir, maxSegSize)
		if err != nil {
			return nil, fmt.Errorf("reload topic %s: %w", name, err)
		}
		b.topics[name] = t
		log.Printf("topic reloaded: %s (next_offset=%d)", name, t.log.NextOffset())
	}
	return b, nil
}

func (b *brokerServer) getTopic(name string) (*topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	return t, ok
}

func (b *brokerServer) CreateTopic(ctx context.Context, req *tp.CreateTopicRequest) (*tp.CreateTopicResponse, error) {
	if req.TopicName == "" {
		return &tp.CreateTopicResponse{Created: false, Error: "topic_name is required"}, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.topics[req.TopicName]; exists {
		return &tp.CreateTopicResponse{Created: false, Error: "topic already exists"}, nil
	}

	t, err := newTopic(req.TopicName, b.dataDir, b.maxSegSize)
	if err != nil {
		return nil, fmt.Errorf("create topic on disk: %w", err)
	}
	b.topics[req.TopicName] = t
	log.Printf("topic created: %s", req.TopicName)
	return &tp.CreateTopicResponse{Created: true}, nil
}

func (b *brokerServer) Publish(ctx context.Context, req *tp.PublishRequest) (*tp.PublishResponse, error) {
	t, ok := b.getTopic(req.TopicName)
	if !ok {
		return nil, fmt.Errorf("topic %q does not exist", req.TopicName)
	}

	msg := &tp.Message{
		Topic: req.TopicName,
		Key:   req.Key,
		Value: req.Value,
	}
	offset, err := t.append(msg)
	if err != nil {
		return nil, fmt.Errorf("append: %w", err)
	}
	return &tp.PublishResponse{Offset: offset}, nil
}

// Subscribe : server streaming.
// Le client envoie une requête d'abonnement, le serveur lui pousse
// les messages au fur et à mesure (et les messages passés si from_offset < taille).
func (b *brokerServer) Subscribe(req *tp.SubscribeRequest, stream tp.Broker_SubscribeServer) error {
	t, ok := b.getTopic(req.TopicName)
	if !ok {
		return fmt.Errorf("topic %q does not exist", req.TopicName)
	}

	consumerID := req.ConsumerId
	if consumerID == "" {
		return fmt.Errorf("consumer_id is required")
	}

	log.Printf("subscribe: consumer=%s topic=%s from_offset=%d",
		consumerID, req.TopicName, req.FromOffset)

	// 1) Replay : envoyer ce qui existait déjà à partir de from_offset
	msgs, err := t.snapshotFrom(req.FromOffset)
	if err != nil {
		return fmt.Errorf("replay: %w", err)
	}
	for _, msg := range msgs {
		if err := stream.Send(msg); err != nil {
			return err
		}
	}

	// 2) Live : s'abonner et streamer les nouveaux messages
	ch := t.addSubscriber(consumerID)
	defer t.removeSubscriber(consumerID)

	ctx := stream.Context()
	for {
		select {
		case <-ctx.Done():
			// Le client s'est déconnecté ou un shutdown est en cours
			log.Printf("subscribe: consumer=%s disconnected", consumerID)
			return nil
		case msg, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(msg); err != nil {
				return err
			}
		}
	}
}

// Commit : un consumer indique au broker qu'il a traité jusqu'à un certain offset.
// Au redémarrage du consumer, il pourra demander cet offset pour reprendre.
func (b *brokerServer) Commit(ctx context.Context, req *tp.CommitRequest) (*tp.CommitResponse, error) {
	if _, ok := b.getTopic(req.TopicName); !ok {
		return nil, fmt.Errorf("topic %q does not exist", req.TopicName)
	}
	if req.ConsumerId == "" {
		return nil, fmt.Errorf("consumer_id is required")
	}

	b.commitMu.Lock()
	defer b.commitMu.Unlock()

	if _, ok := b.commits[req.TopicName]; !ok {
		b.commits[req.TopicName] = make(map[string]int64)
	}

	// On ne recule jamais un offset déjà commité plus haut (sécurité contre les commits en désordre)
	if cur, ok := b.commits[req.TopicName][req.ConsumerId]; ok && req.Offset < cur {
		log.Printf("commit ignored (regression): consumer=%s topic=%s current=%d requested=%d",
			req.ConsumerId, req.TopicName, cur, req.Offset)
		return &tp.CommitResponse{Committed: false}, nil
	}

	b.commits[req.TopicName][req.ConsumerId] = req.Offset
	log.Printf("commit: consumer=%s topic=%s offset=%d", req.ConsumerId, req.TopicName, req.Offset)
	return &tp.CommitResponse{Committed: true}, nil
}

// GetCommittedOffset : un consumer (typiquement au redémarrage) demande son dernier offset commité.
// Retourne exists=false si aucun commit n'existe pour ce couple (topic, consumer_id).
func (b *brokerServer) GetCommittedOffset(ctx context.Context, req *tp.GetCommittedOffsetRequest) (*tp.GetCommittedOffsetResponse, error) {
	if req.ConsumerId == "" {
		return nil, fmt.Errorf("consumer_id is required")
	}

	b.commitMu.RLock()
	defer b.commitMu.RUnlock()

	if topicCommits, ok := b.commits[req.TopicName]; ok {
		if offset, ok := topicCommits[req.ConsumerId]; ok {
			return &tp.GetCommittedOffsetResponse{Offset: offset, Exists: true}, nil
		}
	}
	return &tp.GetCommittedOffsetResponse{Offset: -1, Exists: false}, nil
}
