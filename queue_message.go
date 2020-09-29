package common

import "time"

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
