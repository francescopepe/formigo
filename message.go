package formigo

import "time"

type Message interface {
	ReceivedAt() time.Time
	Content() interface{}
	Id() interface{}
}
