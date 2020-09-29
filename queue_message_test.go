package common

import (
	"testing"
	"time"
)

func TestMarshalAndUnmarshal(t *testing.T) {
	m := QueueMessage{
		Topic:     "abc",
		Partition: 1,
		Offset:    100,
		Key:       []byte("hello"),
		Val:       []byte("world"),
		Time:      time.Now(),
	}
	data, err := m.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}

	m2 := QueueMessage{}
	err = m2.UnmarshalBinary(data)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(m2)
}
