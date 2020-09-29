package common

import (
	"bytes"
	"encoding/binary"
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

	var topicLength int32
	err := binary.Read(buf, binary.BigEndian, &topicLength)
	if err != nil {
		return err
	}
	var topicBytes = make([]byte, topicLength)
	_, err = buf.Read(topicBytes)
	if err != nil {
		return err
	}
	q.Topic = string(topicBytes)

	var partition int32
	err = binary.Read(buf, binary.BigEndian, &partition)
	if err != nil {
		return err
	}
	q.Partition = int(partition)

	err = binary.Read(buf, binary.BigEndian, &q.Offset)
	if err != nil {
		return err
	}

	var keyLength int32
	err = binary.Read(buf, binary.BigEndian, &keyLength)
	if err != nil {
		return err
	}
	var keyBytes = make([]byte, keyLength)
	err = binary.Read(buf, binary.BigEndian, &keyBytes)
	if err != nil {
		return err
	}
	q.Key = keyBytes[:]

	var valLength int32
	err = binary.Read(buf, binary.BigEndian, &valLength)
	if err != nil {
		return err
	}
	var valBytes = make([]byte, valLength)
	err = binary.Read(buf, binary.BigEndian, &valBytes)
	if err != nil {
		return err
	}
	q.Val = valBytes[:]

	var unixNano int64
	err = binary.Read(buf, binary.BigEndian, &unixNano)
	if err != nil {
		return err
	}
	q.Time = time.Unix(0, unixNano)

	return nil
}

func (q QueueMessage) MarshalBinary() (data []byte, err error) {
	buf := bytes.NewBuffer([]byte{})

	dataLength := int32(len(q.Topic))
	err = binary.Write(buf, binary.BigEndian, dataLength)
	if err != nil {
		return
	}
	_, err = buf.WriteString(q.Topic)
	if err != nil {
		return
	}

	err = binary.Write(buf, binary.BigEndian, int32(q.Partition))
	if err != nil {
		return
	}

	err = binary.Write(buf, binary.BigEndian, q.Offset)
	if err != nil {
		return
	}

	keyLength := int32(len(q.Key))
	err = binary.Write(buf, binary.BigEndian, keyLength)
	if err != nil {
		return
	}
	err = binary.Write(buf, binary.BigEndian, q.Key)
	if err != nil {
		return
	}

	valLength := int32(len(q.Val))
	err = binary.Write(buf, binary.BigEndian, valLength)
	if err != nil {
		return
	}
	err = binary.Write(buf, binary.BigEndian, q.Val)
	if err != nil {
		return
	}

	err = binary.Write(buf, binary.BigEndian, q.Time.UnixNano())
	if err != nil {
		return
	}

	data = buf.Bytes()
	return
}
