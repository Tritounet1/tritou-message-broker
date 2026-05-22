package api

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"path/filepath"

	tp "tidy/topic"
)

type Segment struct {
	file       *os.File
	writer     *bufio.Writer
	baseOffset int64
	size       int64
}

// newSegment ouvre (ou crée) un fichier segment pour ce baseOffset.
func newSegment(dir string, baseOffset int64) (*Segment, error) {
	path := filepath.Join(dir, fmt.Sprintf("%020d.log", baseOffset))
	f, err := os.OpenFile(path, os.O_APPEND|os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("open segment %s: %w", path, err)
	}
	stat, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Segment{
		file:       f,
		writer:     bufio.NewWriter(f),
		baseOffset: baseOffset,
		size:       stat.Size(),
	}, nil
}

// Append écrit un record encodé. Met à jour size.
func (s *Segment) Append(data []byte) error {
	n, err := s.writer.Write(data)
	if err != nil {
		return err
	}
	s.size += int64(n)
	return nil
}

// Flush force l'écriture du buffer vers le fichier.
// (Sans fsync, pas de garantie disque. C'est un trade-off perf vs durabilité.)
func (s *Segment) Flush() error {
	return s.writer.Flush()
}

// Close flush et ferme le fichier.
func (s *Segment) Close() error {
	if err := s.writer.Flush(); err != nil {
		s.file.Close()
		return err
	}
	return s.file.Close()
}

// ReadAll relit tous les records du segment depuis le début.
// Utilisé au démarrage pour reconstruire l'état en mémoire.
func (s *Segment) ReadAll() ([]*tp.Message, error) {
	// On ouvre un reader indépendant pour ne pas perturber le writer.
	f, err := os.Open(s.file.Name())
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReader(f)
	var messages []*tp.Message
	for {
		msg, err := readRecord(reader)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read record from %s: %w", s.file.Name(), err)
		}
		messages = append(messages, msg)
	}
	return messages, nil
}
