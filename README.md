# Message Broker (GoStream)

A lire :

Link de ce que c'est un Message Broker : https://medium.com/@cindanojonathan.fr/message-broker-microservices-7a23ef865435
Le plus connu : https://kafka.apache.org/
Explication de kafka : https://medium.com/@aymen.zeidi/comprendre-apache-kafka-un-broker-de-messages-haute-performance-e864b7e2a6ad

## Pourquoi ce projet

Je suis personnelement très tourné vers le développement de service backend, je suis aussi beaucoup intérrésé par Docker et son omni-présence dans environnements de production.

##

Pourquoi celui-là : il coche absolument toutes les cases que les recruteurs backend regardent (concurrence, I/O, réseau, persistance, systèmes distribués, performance), il est unique sur un CV (90% des candidats ont une todo-list ou un blog), et tu peux l'étendre indéfiniment selon ton ambition.
Roadmap progressive en 4 phases
Phase 1 — Broker single-node (1-2 semaines)
Le cœur d'un broker : un serveur TCP qui accepte publishers et subscribers, des topics en mémoire, un protocole binaire custom (style Kafka wire protocol simplifié). Tu utilises net.Listener, des goroutines par connexion, et des channels pour distribuer les messages.
Concepts clés à maîtriser : framing TCP (length-prefixed messages), backpressure avec channels bufferisés, graceful shutdown avec context.Context.
Phase 2 — Persistance sur disque (1-2 semaines)
Tu ajoutes un commit log par topic : segments de fichiers append-only, index par offset, retention par taille ou par temps. C'est exactement ce que fait Kafka.
Concepts : mmap pour les segments, bufio.Writer avec flush périodique, format binaire des records (header + payload + checksum CRC32).
Phase 3 — Partitions & consumer groups (2 semaines)
Chaque topic a N partitions, et les consumers d'un même groupe se partagent les partitions. Tu implémentes un coordinateur qui assigne les partitions et gère les rebalances quand un consumer rejoint ou quitte.
Concepts : hashing pour le partitioning, offset management, heartbeats, rebalance protocol.
Phase 4 — Cluster & réplication (3-4 semaines, le morceau qui impressionne)
Plusieurs brokers en cluster, chaque partition a un leader et des followers, élection du leader via Raft (utilise la lib hashicorp/raft pour ne pas réinventer ça). Quand le leader tombe, un follower prend le relais.
Concepts : Raft consensus, ISR (in-sync replicas), idempotent producers, exactly-once semantics (au moins en théorie).
Ce que tu mets sur le CV après ça

GoStream — Message broker distribué en Go inspiré de Kafka

Architecture multi-broker avec réplication via Raft consensus (3+ nodes)
Commit log persistant avec segments, retention policies et compaction
Partitions, consumer groups avec rebalancing automatique
Throughput : X messages/sec en single-node, latence p99 < Yms (benchmarks)
Stack : Go, gRPC, Raft (hashicorp), Prometheus, Docker
[Lien GitHub avec README détaillé + architecture diagrams + benchmarks]

Les chiffres en gras (throughput, latence) sont cruciaux : ça montre que tu sais benchmarker et que tu raisonnes en perf.
Compétences que tu démontres
Concurrence Go avancée (goroutines, channels, sync primitives, context propagation), I/O performant (buffered I/O, mmap, zero-copy quand possible), protocoles réseau bas niveau (TCP custom ou gRPC), systèmes distribués (consensus, réplication, failure detection), persistance et storage engines, observabilité (metrics Prometheus, tracing OpenTelemetry, structured logging).
C'est littéralement la stack de Confluent, RedPanda, NATS — des boîtes qui recrutent très bien.
Comment maximiser l'impact CV
Un README exceptionnel : diagramme d'architecture, explication des design decisions, benchmarks avec graphes, comparaison vs Kafka/NATS sur certains aspects. Tu peux aussi écrire 2-3 articles de blog sur les parties les plus tricky (par exemple "Implementing Raft in Go: lessons learned", "Building a commit log: from naive to fast"). Et ajoute des tests sérieux : property-based testing avec gopter, chaos testing (kill un broker random, vérifier que ça tient).

Planning des phases de GoStream
Phase 1 — Broker single-node
À utiliser : net (TCP), bufio, encoding/binary, context, sync, goroutines + channels, log/slog pour les logs structurés.
Rendu attendu :

Serveur TCP qui accepte plusieurs connexions concurrentes
Protocole binaire custom (header avec magic number, version, type de message, length, payload)
Commandes supportées : CREATE_TOPIC, PUBLISH, SUBSCRIBE, ACK
Topics stockés en mémoire (map protégée par RWMutex ou sharded map)
Un publisher peut envoyer des messages, plusieurs subscribers les reçoivent
Graceful shutdown propre (Ctrl+C ferme les connexions sans perdre de messages en cours)
Client CLI simple pour tester (gostream-cli publish topic1 "hello")
Tests unitaires sur le protocole et l'encoding/decoding

Phase 2 — Persistance sur disque
À utiliser : os, io, bufio, hash/crc32, syscall ou golang.org/x/sys pour mmap (optionnel), structures binaires custom.
Rendu attendu :

Commit log par topic : dossier par topic, segments de fichiers (00000000.log, 00000001.log...)
Format des records : [length|timestamp|crc32|key_size|key|value_size|value]
Rotation automatique des segments (nouveau fichier au-delà de X MB)
Index par offset (00000000.index) pour retrouver un message rapidement
Retention policy : suppression des vieux segments par taille totale ou par âge
Replay au démarrage : reconstruction de l'état depuis les fichiers
Flush périodique vs sync (configurable : durabilité vs perf)
Benchmark : mesurer le throughput write avec et sans fsync

Phase 3 — Partitions & consumer groups
À utiliser : hash/fnv ou hash/maphash pour le partitioning, sync.Map ou maps custom, channels pour la coordination, peut-être déjà gRPC (google.golang.org/grpc) pour remplacer ton protocole custom.
Rendu attendu :

Chaque topic est divisé en N partitions (configurable à la création)
Producer : choix de la partition par clé (hash) ou round-robin si pas de clé
Consumer groups : plusieurs consumers d'un même groupe se partagent les partitions
Coordinator qui assigne les partitions aux consumers
Heartbeats : un consumer qui ne répond plus est retiré du groupe
Rebalance automatique quand un consumer rejoint/quitte
Offset commits stockés sur disque (un topic interne \_\_consumer_offsets comme Kafka)
Reprise après crash : un consumer redémarré reprend là où il s'était arrêté

Phase 4 — Cluster & réplication
À utiliser : github.com/hashicorp/raft + raft-boltdb pour le store, gRPC pour la communication inter-broker, prometheus/client_golang pour les métriques, opentelemetry-go pour le tracing.
Rendu attendu :

Cluster de N brokers (minimum 3) qui se découvrent
Chaque partition a un leader et des replicas (replication factor configurable)
Élection du leader via Raft
Les writes vont au leader, qui réplique aux followers avant d'ACK
ISR (In-Sync Replicas) : seuls les followers à jour peuvent devenir leader
Failover automatique : si un broker meurt, ses partitions sont réélues sur d'autres
Métriques Prometheus : throughput, latence, lag par consumer, état du cluster
Dashboard Grafana fourni
Docker Compose pour lancer un cluster local de 3 brokers
Chaos testing : un script qui kill un broker random et vérifie que ça tient

C'est quoi un message broker, concrètement ?
Tu as exactement la bonne intuition : oui, c'est un composant central des architectures microservices, mais c'est plus large que ça. Laisse-moi t'expliquer ce que ça résout.
Le problème de base
Imagine une appli e-commerce avec des microservices : OrderService, PaymentService, EmailService, InventoryService, AnalyticsService.
Quand un client passe commande, il faut :

Débiter le paiement
Décrémenter le stock
Envoyer un email de confirmation
Mettre à jour les analytics
Notifier le service de livraison

Approche naïve (sans broker) : OrderService fait des appels HTTP directs aux 5 autres services. Problèmes :

Si EmailService est down, est-ce que la commande échoue ? Non, on s'en fout d'un email pour valider une commande. Mais en HTTP synchrone, faut gérer ça à la main partout.
Si AnalyticsService met 3 secondes à répondre, le client attend 3 secondes de plus pour rien.
OrderService doit connaître l'adresse de tous les autres services (couplage fort).
Si tu ajoutes un nouveau service (FraudDetectionService), tu dois modifier OrderService.

Avec un message broker
OrderService publie un message OrderCreated dans un topic. Il s'en va, il a fini son boulot. Le broker s'occupe du reste.
Tous les services intéressés (Payment, Email, Inventory...) sont abonnés au topic et reçoivent le message chacun de leur côté, à leur rythme. OrderService ne sait même pas qui écoute.
Les bénéfices concrets :

Découplage : les services ne se connaissent pas entre eux, ils ne connaissent que le broker
Asynchrone : OrderService répond au client en 50ms même si AnalyticsService met 10s à traiter
Résilience : si EmailService est down 2h, les messages s'accumulent dans le broker, et il les traite tous quand il revient. Aucune perte.
Scalabilité : tu peux lancer 10 instances d'EmailService en parallèle, le broker répartit la charge automatiquement (consumer groups)
Extensibilité : ajouter un nouveau service consommateur = zéro modif sur les producers

Les autres usages au-delà des microservices
Un broker comme Kafka ne sert pas qu'aux microservices. C'est aussi :

Event sourcing : tu stockes tous les événements métier (le log devient ta source de vérité, tu peux reconstruire n'importe quel état en rejouant les events)
Stream processing : analyse en temps réel de flux massifs (logs, métriques, clics utilisateurs, transactions financières)
Data pipelines : ingestion de données depuis des sources hétérogènes (apps, bases, capteurs IoT) vers des entrepôts (data lake, data warehouse)
Change Data Capture : tu captures tous les changements d'une base de données et tu les propages ailleurs (Debezium fait ça avec Kafka)
Découplage temporel : un système produit à 100k msg/s pendant 5 minutes, le consommateur traite à 10k msg/s pendant 50 minutes, sans rien perdre
