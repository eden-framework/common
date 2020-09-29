package common

import (
	"bytes"
	"encoding/gob"
	"time"
)

type QueueMessage struct {
	// Topic is reads only and MUST NOT be set when writing messages
	Topic string
	// Partition is reads only and MUST NOT be set when writing messages
	Partition int
	Offset    int64
	Key       []byte
	Val       []byte
	Time      time.Time
}

func (q *QueueMessage) UnmarshalBinary(data []byte) error {
	buf := bytes.NewReader(data)
	decoder := gob.NewDecoder(buf)
	return decoder.Decode(q)
}

func (q QueueMessage) MarshalBinary() (data []byte, err error) {
	buf := bytes.NewBuffer([]byte{})
	encoder := gob.NewEncoder(buf)
	err = encoder.Encode(q)
	if err != nil {
		return
	}
	data = buf.Bytes()
	return
}
