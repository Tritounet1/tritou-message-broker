package api

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	tp "tidy/topic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func RunClient() {
	// Connexion au broker
	conn, err := grpc.NewClient(
		"localhost:50051",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	client := tp.NewBrokerClient(conn)

	// 1) Création du topic
	topicName := "orders"
	consumerID := "consumer-1"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	createResp, err := client.CreateTopic(ctx, &tp.CreateTopicRequest{
		TopicName: topicName,
	})
	if err != nil {
		log.Fatalf("CreateTopic failed: %v", err)
	}
	if !createResp.Created {
		// Si le topic existe déjà (relance du client), on continue
		log.Printf("CreateTopic: %s (continuing)", createResp.Error)
	} else {
		log.Printf("✓ topic %q created", topicName)
	}

	// 2) Lancer un subscriber en parallèle
	// Le subscriber tourne dans une goroutine et imprime les messages reçus.
	subDone := make(chan struct{})
	subCtx, subCancel := context.WithCancel(context.Background())
	defer subCancel()

	go runSubscriber(subCtx, client, topicName, consumerID, 0, subDone)

	// Laisser le subscriber le temps de s'enregistrer côté serveur
	time.Sleep(200 * time.Millisecond)

	// 3) Publier quelques messages
	log.Println("--- publishing 5 messages ---")
	for i := 0; i < 5; i++ {
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.Publish(pubCtx, &tp.PublishRequest{
			TopicName: topicName,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("hello message #%d", i)),
		})
		pubCancel()
		if err != nil {
			log.Fatalf("Publish failed: %v", err)
		}
		log.Printf("→ published msg #%d at offset=%d", i, resp.Offset)
	}

	// Laisser le temps au subscriber recevoir tous les messages
	time.Sleep(500 * time.Millisecond)

	// 4) Commit jusqu'à l'offset 2
	// On simule que le consumer a traité jusqu'au message à l'offset 2 et veut le mémoriser.
	log.Println("--- committing offset 2 ---")
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 2*time.Second)
	commitResp, err := client.Commit(commitCtx, &tp.CommitRequest{
		TopicName:  topicName,
		ConsumerId: consumerID,
		Offset:     2,
	})
	commitCancel()
	if err != nil {
		log.Fatalf("Commit failed: %v", err)
	}
	log.Printf("✓ commit ok: %v", commitResp.Committed)

	// 5) Arrêter le premier subscriber (simulation crash)
	log.Println("--- stopping subscriber (simulating crash) ---")
	subCancel()
	<-subDone

	// 6) Récupérer l'offset commité et reprendre
	getCtx, getCancel := context.WithTimeout(context.Background(), 2*time.Second)
	offResp, err := client.GetCommittedOffset(getCtx, &tp.GetCommittedOffsetRequest{
		TopicName:  topicName,
		ConsumerId: consumerID,
	})
	getCancel()
	if err != nil {
		log.Fatalf("GetCommittedOffset failed: %v", err)
	}

	var resumeFrom int64
	if offResp.Exists {
		// Le consumer a déjà traité jusqu'à offResp.Offset, donc il reprend au suivant.
		resumeFrom = offResp.Offset + 1
		log.Printf("✓ last committed offset = %d, resuming from %d", offResp.Offset, resumeFrom)
	} else {
		resumeFrom = 0
		log.Printf("no committed offset, starting from 0")
	}

	// 7) Relancer un subscriber depuis le bon offset
	log.Println("--- restarting subscriber from committed offset ---")
	sub2Done := make(chan struct{})
	sub2Ctx, sub2Cancel := context.WithCancel(context.Background())

	go runSubscriber(sub2Ctx, client, topicName, consumerID, resumeFrom, sub2Done)

	// Laisser le replay se faire
	time.Sleep(300 * time.Millisecond)

	// 8) Publier 2 nouveaux messages pour voir le live
	log.Println("--- publishing 2 more messages (live) ---")
	for i := 5; i < 7; i++ {
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.Publish(pubCtx, &tp.PublishRequest{
			TopicName: topicName,
			Key:       []byte(fmt.Sprintf("key-%d", i)),
			Value:     []byte(fmt.Sprintf("hello message #%d", i)),
		})
		pubCancel()
		if err != nil {
			log.Fatalf("Publish failed: %v", err)
		}
		log.Printf("→ published msg #%d at offset=%d", i, resp.Offset)
	}

	// Laisser le subscriber recevoir
	time.Sleep(500 * time.Millisecond)

	// 9) Nettoyage
	log.Println("--- shutting down ---")
	sub2Cancel()
	<-sub2Done
	log.Println("✓ done")
}

// runSubscriber ouvre un stream Subscribe et imprime tous les messages reçus
// jusqu'à ce que le contexte soit annulé ou que le stream se ferme.
func runSubscriber(
	ctx context.Context,
	client tp.BrokerClient,
	topicName string,
	consumerID string,
	fromOffset int64,
	done chan<- struct{},
) {
	defer close(done)

	stream, err := client.Subscribe(ctx, &tp.SubscribeRequest{
		TopicName:  topicName,
		ConsumerId: consumerID,
		FromOffset: fromOffset,
	})
	if err != nil {
		log.Printf("[sub] Subscribe failed: %v", err)
		return
	}

	log.Printf("[sub] subscribed: topic=%s consumer=%s from=%d", topicName, consumerID, fromOffset)

	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			log.Printf("[sub] stream closed by server")
			return
		}
		if err != nil {
			// Quand on annule le contexte, on tombe ici avec une erreur "context canceled"
			if ctx.Err() != nil {
				log.Printf("[sub] stream stopped (context canceled)")
				return
			}
			log.Printf("[sub] Recv error: %v", err)
			return
		}
		log.Printf("[sub] ← received offset=%d key=%q value=%q ts=%d",
			msg.Offset, string(msg.Key), string(msg.Value), msg.TimestampMs)
	}
}
