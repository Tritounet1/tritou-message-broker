package api

import "time"

type RetentionPolicy struct {
	MaxAge   time.Duration // ex: 7 * 24h
	MaxBytes int64         // ex: 10 GB
}

/*
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
