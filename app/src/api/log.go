package api

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"

	tp "tidy/topic"
)

type Log struct {
	mu         sync.Mutex
	dir        string
	maxSegSize int64
	segments   []*Segment // triés par baseOffset, le dernier est l'actif
	nextOffset int64      // prochain offset à assigner
}

// openLog ouvre (ou crée) un log dans le dossier dir.
// Si des segments existent déjà, ils sont chargés et nextOffset est recalculé.
func openLog(dir string, maxSegSize int64) (*Log, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("mkdir %s: %w", dir, err)
	}

	l := &Log{
		dir:        dir,
		maxSegSize: maxSegSize,
	}

	// Lister les fichiers .log existants
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var baseOffsets []int64
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".log") {
			continue
		}
		offsetStr := strings.TrimSuffix(name, ".log")
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			continue // fichier non conforme, on l'ignore
		}
		baseOffsets = append(baseOffsets, offset)
	}
	sort.Slice(baseOffsets, func(i, j int) bool { return baseOffsets[i] < baseOffsets[j] })

	// Ouvrir tous les segments existants
	for _, off := range baseOffsets {
		seg, err := newSegment(dir, off)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, seg)
	}

	// Si aucun segment, en créer un à l'offset 0
	if len(l.segments) == 0 {
		seg, err := newSegment(dir, 0)
		if err != nil {
			return nil, err
		}
		l.segments = append(l.segments, seg)
		l.nextOffset = 0
		return l, nil
	}

	// Sinon, calculer nextOffset en relisant le dernier segment
	active := l.segments[len(l.segments)-1]
	msgs, err := active.ReadAll()
	if err != nil {
		return nil, err
	}
	l.nextOffset = active.baseOffset + int64(len(msgs))
	return l, nil
}

// Append encode et écrit un message, retourne l'offset assigné.
func (l *Log) Append(msg *tp.Message) (int64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	active := l.segments[len(l.segments)-1]

	// Rotation si plein
	if active.size >= l.maxSegSize {
		if err := active.Flush(); err != nil {
			return 0, err
		}
		newSeg, err := newSegment(l.dir, l.nextOffset)
		if err != nil {
			return 0, err
		}
		l.segments = append(l.segments, newSeg)
		active = newSeg
	}

	offset := l.nextOffset
	msg.Offset = offset

	data := encodeRecord(msg)
	if err := active.Append(data); err != nil {
		return 0, err
	}

	// Flush à chaque write pour simplicité (durabilité raisonnable).
	// Optimisation future : flush périodique en arrière-plan.
	if err := active.Flush(); err != nil {
		return 0, err
	}

	l.nextOffset++
	return offset, nil
}

// ReadFrom lit tous les messages à partir de fromOffset jusqu'au dernier.
// Utilisé pour le replay au moment du Subscribe.
func (l *Log) ReadFrom(fromOffset int64) ([]*tp.Message, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	var result []*tp.Message
	for _, seg := range l.segments {
		// Skip les segments entièrement antérieurs à fromOffset
		// (on charge l'intégralité du segment puis on filtre, simple et suffisant pour la Phase 2)
		msgs, err := seg.ReadAll()
		if err != nil {
			return nil, err
		}
		for i, m := range msgs {
			msgOffset := seg.baseOffset + int64(i)
			if msgOffset < fromOffset {
				continue
			}
			m.Offset = msgOffset
			result = append(result, m)
		}
	}
	return result, nil
}

// NextOffset retourne le prochain offset (= dernier + 1).
func (l *Log) NextOffset() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextOffset
}

// Close ferme tous les segments.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, seg := range l.segments {
		if err := seg.Close(); err != nil {
			return err
		}
	}
	return nil
}

// segmentFilename calcule le nom de fichier pour un baseOffset donné.
// (Utilitaire au cas où...)
func segmentFilename(dir string, baseOffset int64) string {
	return filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
}
