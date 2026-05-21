package api

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	tp "tidy/topic"

	"google.golang.org/grpc"
)

// Modèle en mémoire
// topic représente un topic et ses messages stockés en mémoire.
type topic struct {
	mu       sync.RWMutex
	name     string
	messages []*tp.Message // ordonnés par offset, l'index = l'offset
	// fanout : chaque subscriber a un channel sur lequel on lui pousse les nouveaux messages
	subscribers map[string]chan *tp.Message
}

func newTopic(name string) *topic {
	return &topic{
		name:        name,
		messages:    make([]*tp.Message, 0, 1024),
		subscribers: make(map[string]chan *tp.Message),
	}
}

// append ajoute un message au topic et le pousse aux subscribers actifs.
// Retourne l'offset assigné.
func (t *topic) append(msg *tp.Message) int64 {
	t.mu.Lock()
	offset := int64(len(t.messages))
	msg.Offset = offset
	msg.TimestampMs = time.Now().UnixMilli()
	t.messages = append(t.messages, msg)

	// Snapshot des subscribers pour fanout hors du lock principal
	subs := make([]chan *tp.Message, 0, len(t.subscribers))
	for _, ch := range t.subscribers {
		subs = append(subs, ch)
	}
	t.mu.Unlock()

	// Fanout non-bloquant : si un subscriber est trop lent, on drop pour lui.
	for _, ch := range subs {
		select {
		case ch <- msg:
		default:
		}
	}
	return offset
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

// snapshotFrom retourne une copie des messages à partir d'un offset donné.
// Utile pour replay quand un consumer demande from_offset = 0.
func (t *topic) snapshotFrom(offset int64) []*tp.Message {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if offset >= int64(len(t.messages)) {
		return nil
	}
	out := make([]*tp.Message, len(t.messages)-int(offset))
	copy(out, t.messages[offset:])
	return out
}

// Broker
type brokerServer struct {
	tp.UnimplementedBrokerServer

	mu     sync.RWMutex
	topics map[string]*topic

	// offsets commités : topic -> consumer_id -> dernier offset commité
	// Mutex séparé pour ne pas bloquer les publishs quand un consumer commit.
	commitMu sync.RWMutex
	commits  map[string]map[string]int64
}

func newBrokerServer() *brokerServer {
	return &brokerServer{
		topics:  make(map[string]*topic),
		commits: make(map[string]map[string]int64),
	}
}

func (b *brokerServer) getTopic(name string) (*topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	return t, ok
}

// CreateTopic : crée un topic s'il n'existe pas déjà.
func (b *brokerServer) CreateTopic(ctx context.Context, req *tp.CreateTopicRequest) (*tp.CreateTopicResponse, error) {
	if req.TopicName == "" {
		return &tp.CreateTopicResponse{Created: false, Error: "topic_name is required"}, nil
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	if _, exists := b.topics[req.TopicName]; exists {
		return &tp.CreateTopicResponse{Created: false, Error: "topic already exists"}, nil
	}
	b.topics[req.TopicName] = newTopic(req.TopicName)
	log.Printf("topic created: %s", req.TopicName)
	return &tp.CreateTopicResponse{Created: true}, nil
}

// Publish : ajoute un message à un topic et retourne son offset.
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
	offset := t.append(msg)
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
	for _, msg := range t.snapshotFrom(req.FromOffset) {
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

// main
func RunServer() {
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	s := grpc.NewServer()
	tp.RegisterBrokerServer(s, newBrokerServer())

	// Signal Ctrl+C pour arrêt propre
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		log.Printf("server listening at %v", lis.Addr())
		if err := s.Serve(lis); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
	}()

	<-stop
	log.Println("shutting down gracefully...")
	s.GracefulStop() // laisser les streams en cours se terminer
	log.Println("server stopped")
}

/*
// Config par topic
type RetentionPolicy struct {
	MaxAge   time.Duration // ex: 7 * 24h
	MaxBytes int64         // ex: 10 GB
}

pour RetentionPolicy

il faut que ça soit configurable donc on a :
- choix de la durée de vie des messages (le temps qu'on laisse les messages en vie)
- choix de la taille maximale sur le disque de tout les messages (si on dépasse la taille on supprime le premier segment du disque)
- choix de ne rien supprimer

// Goroutine qui tourne toutes les minutes
func (b *brokerServer) runRetentionCleaner() {
	ticker := time.NewTicker(time.Minute)
	for range ticker.C {
		for _, topic := range b.topics {
			// TODO: supprimés les messages selon la RetentionPolicy du brokerServer
			topic.cleanOldSegments()
		}
	}
}
*/
