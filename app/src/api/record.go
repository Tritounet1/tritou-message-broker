package api

import (
	"encoding/binary"
	"fmt"
	"io"

	tp "tidy/topic"
)

// Format binaire d'un record sur disque :
//   [length: 4B BE]    taille du reste du record (timestamp + key + value + leurs sizes)
//   [timestamp: 8B BE] timestamp en ms
//   [key_size: 4B BE]
//   [key: N bytes]
//   [value_size: 4B BE]
//   [value: M bytes]

func encodeRecord(msg *tp.Message) []byte {
	keyLen := len(msg.Key)
	valLen := len(msg.Value)

	// taille du payload après les 4 bytes de length
	payloadSize := 8 + 4 + keyLen + 4 + valLen

	buf := make([]byte, 4+payloadSize)
	binary.BigEndian.PutUint32(buf[0:4], uint32(payloadSize))
	binary.BigEndian.PutUint64(buf[4:12], uint64(msg.TimestampMs))
	binary.BigEndian.PutUint32(buf[12:16], uint32(keyLen))
	copy(buf[16:16+keyLen], msg.Key)
	off := 16 + keyLen
	binary.BigEndian.PutUint32(buf[off:off+4], uint32(valLen))
	copy(buf[off+4:], msg.Value)

	return buf
}

// readRecord lit un record depuis un Reader. Retourne io.EOF proprement quand on a fini.
func readRecord(r io.Reader) (*tp.Message, error) {
	// 1) Lire la longueur
	lengthBuf := make([]byte, 4)
	if _, err := io.ReadFull(r, lengthBuf); err != nil {
		return nil, err // io.EOF si on est en fin de fichier proprement
	}
	payloadSize := binary.BigEndian.Uint32(lengthBuf)

	// 2) Lire le payload d'un coup
	payload := make([]byte, payloadSize)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, fmt.Errorf("read payload: %w", err)
	}

	// 3) Décoder
	timestamp := int64(binary.BigEndian.Uint64(payload[0:8]))
	keyLen := binary.BigEndian.Uint32(payload[8:12])

	if uint32(len(payload)) < 12+keyLen+4 {
		return nil, fmt.Errorf("corrupted record: key too large")
	}
	key := make([]byte, keyLen)
	copy(key, payload[12:12+keyLen])

	off := 12 + keyLen
	valLen := binary.BigEndian.Uint32(payload[off : off+4])
	if uint32(len(payload)) < off+4+valLen {
		return nil, fmt.Errorf("corrupted record: value too large")
	}
	value := make([]byte, valLen)
	copy(value, payload[off+4:off+4+valLen])

	return &tp.Message{
		TimestampMs: timestamp,
		Key:         key,
		Value:       value,
	}, nil
}
