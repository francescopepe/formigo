package formigo

import "time"

type Message interface {
	ReceivedAt() time.Time
	Raw() interface{}
	Content() interface{}
}
