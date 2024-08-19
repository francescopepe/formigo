package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/assert"

	smithyendpoints "github.com/aws/smithy-go/endpoints"

	"github.com/francescopepe/formigo"
	"github.com/francescopepe/formigo/internal/messages"
)

type LocalStackEndpointResolver struct{}

func (r *LocalStackEndpointResolver) ResolveEndpoint(ctx context.Context, params sqs.EndpointParameters) (
	smithyendpoints.Endpoint, error,
) {
	endpoint := smithyendpoints.Endpoint{
		Headers: http.Header{},
	}

	uriString := "http://localhost:4566"
	uri, err := url.Parse(uriString)
	if err != nil {
		return endpoint, fmt.Errorf("Failed to parse uri: %s", uriString)
	}

	endpoint.URI = *uri

	return endpoint, nil
}

type SqsClient struct {
	client *sqs.Client
}

func NewSqsClient(awsSqsClient *sqs.Client) SqsClient {
	return SqsClient{awsSqsClient}
}

func (c *SqsClient) CreateQueue(ctx context.Context, queueName string) (string, error) {
	queue, err := c.client.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String(queueName),
	})
	if err != nil {
		return "", fmt.Errorf("failed to create queue %s: %w", queueName, err)
	}

	return *queue.QueueUrl, nil
}

func (c *SqsClient) DeleteQueue(ctx context.Context, queueUrl string) error {
	_, err := c.client.DeleteQueue(ctx, &sqs.DeleteQueueInput{
		QueueUrl: aws.String(queueUrl)})
	if err != nil {
		return fmt.Errorf("failed to delete queue %s: %w", queueUrl, err)
	}

	return nil
}

func (c *SqsClient) SendMessages(ctx context.Context, queueUrl string, msgs []string) error {
	entries := make([]types.SendMessageBatchRequestEntry, 0, 10)
	for _, msg := range msgs {
		entries = append(entries, types.SendMessageBatchRequestEntry{
			Id:          aws.String(fmt.Sprintf("%d", time.Now().UnixNano())),
			MessageBody: aws.String(msg),
		})
	}
	_, err := c.client.SendMessageBatch(ctx, &sqs.SendMessageBatchInput{
		QueueUrl: &queueUrl,
		Entries:  entries,
	})
	if err != nil {
		return fmt.Errorf("failed to send messages: %w", err)
	}

	return nil
}

func (c *SqsClient) ReceiveMessages(ctx context.Context, queueUrl string) ([]string, error) {
	resp, err := c.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
		QueueUrl:            &queueUrl,
		MaxNumberOfMessages: 10,
		WaitTimeSeconds:     20,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to receive messages: %w", err)
	}
	msgs := make([]string, 0, len(resp.Messages))
	for _, msg := range resp.Messages {
		msgs = append(msgs, *msg.Body)
	}

	return msgs, nil
}

func TestSqs(t *testing.T) {
	ctx, cancelCtx := context.WithCancel(context.Background())

	awsCfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithDefaultRegion("us-east-1"),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "session string")),
	)
	if err != nil {
		t.Fatalf("unable to create AWS config: %s", err)
	}

	awsSqsClient := sqs.NewFromConfig(awsCfg, sqs.WithEndpointResolverV2(&LocalStackEndpointResolver{}))

	sqsClient := NewSqsClient(awsSqsClient)

	queueUrl, err := sqsClient.CreateQueue(ctx, "test-fra")
	if err != nil {
		t.Fatalf("unable to create AWS config: %s", err)
	}
	defer sqsClient.DeleteQueue(ctx, queueUrl)

	err = sqsClient.SendMessages(ctx, queueUrl, []string{"msg1", "msg2"})
	if err != nil {
		t.Fatalf("unable to send SQS messages: %s", err)
	}

	// msgs, err := sqsClient.ReceiveMessages(ctx, queueUrl)
	// if err != nil {
	// 	t.Errorf("unable to receive SQS messages: %s", err)
	// }

	// assert.Equal(t, msgs, []string{"msg1"})

	sqsFormigoClient, err := formigo.NewSqsClient(ctx, formigo.SqsClientConfiguration{
		MessageCtxTimeout: time.Second * 10,
		Svc:               awsSqsClient,
		ReceiveMessageInput: &sqs.ReceiveMessageInput{
			QueueUrl:            &queueUrl,
			MaxNumberOfMessages: 1,
			WaitTimeSeconds:     1,
		},
	})
	if err != nil {
		t.Fatalf("unable to create sqs client: %s", err)
	}

	worker := formigo.NewWorker(formigo.Configuration{
		Client:      sqsFormigoClient,
		Concurrency: 1,
		Retrievers:  1,
		Consumer: formigo.NewMessageConsumer(formigo.MessageConsumerConfiguration{
			Handler: func(ctx context.Context, msg formigo.Message) error {
				encoded, err := json.Marshal(msg)
				if err != nil {
					return fmt.Errorf("unable to JSON encode: %w", err)
				}

				log.Println(string(encoded))
				cancelCtx()

				return nil
			},
		}),
	})

	err = worker.Run(ctx)
	assert.Nil(t, err)
}

func getMessageIds(msgs []messages.Message) []string {
	ids := make([]string, 0, len(msgs))
	for _, msg := range msgs {
		ids = append(ids, msg.MsgId.(string))
	}

	return ids
}

func TestFra(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := NewSimpleInMemoryBroker()

	go broker.run(ctx)
	log.Println("Broker started")

	broker.AddMessages([]*SimpleInMemoryBrokerMessage{
		{
			MessageId: "message1",
			Body:      "This is the body1",
		},
		{
			MessageId: "message2",
			Body:      "This is the body2",
		},
	})

	// msgs, _ := broker.ReceiveMessages()
	// log.Println("Got message", getMessageIds(msgs))

	// <-time.After(time.Second * 1)
	// _ = broker.DeleteMessages(msgs)

	// broker.AddMessages([]*SimpleInMemoryBrokerMessage{
	// 	{
	// 		MessageId: "message1",
	// 		Body:      "This is the body",
	// 	},
	// })
	// broker.AddMessages([]*SimpleInMemoryBrokerMessage{
	// 	{
	// 		MessageId: "message2",
	// 		Body:      "This is the body",
	// 	},
	// })
	// msgs, _ = broker.ReceiveMessages()
	// log.Println("Got message", getMessageIds(msgs))

	// <-time.After(time.Second * 7)
	// msgs, _ = broker.ReceiveMessages()
	// log.Println("Got message", getMessageIds(msgs))

	// msgs, _ = broker.ReceiveMessages()
	// log.Println("Got message", getMessageIds(msgs))

	// <-time.After(time.Second * 30)
	// cancel()
	// log.Println("Broker stopped")

	worker := formigo.NewWorker(formigo.Configuration{
		Client:      broker,
		Concurrency: 1,
		Retrievers:  1,
		Consumer: formigo.NewBatchConsumer(formigo.BatchConsumerConfiguration{
			BufferConfig: formigo.BatchConsumerBufferConfiguration{
				Size:    1,
				Timeout: time.Second,
			},
			Handler: func(ctx context.Context, msgs []formigo.Message) (formigo.BatchResponse, error) {
				encoded, err := json.Marshal(msgs)
				if err != nil {
					return formigo.BatchResponse{}, fmt.Errorf("unable to JSON encode: %w", err)
				}

				log.Println(len(msgs), string(encoded))
				cancel()

				return formigo.BatchResponse{}, nil
			},
		}),
		// Consumer: formigo.NewMessageConsumer(formigo.MessageConsumerConfiguration{
		// 	Handler: func(ctx context.Context, msg formigo.Message) error {
		// 		encoded, err := json.Marshal(msg)
		// 		if err != nil {
		// 			return fmt.Errorf("unable to JSON encode: %w", err)
		// 		}

		// 		log.Println(string(encoded))
		// 		cancel()

		// 		return nil
		// 	},
		// }),
	})

	err := worker.Run(ctx)
	assert.Nil(t, err)
}

type SimpleInMemoryBrokerMessage struct {
	MessageId    string
	Body         string
	deleted      bool
	enqueueAfter <-chan time.Time
}

type SimpleInMemoryBroker struct {
	queue     chan *SimpleInMemoryBrokerMessage
	inFlights chan *SimpleInMemoryBrokerMessage
	expired   chan *SimpleInMemoryBrokerMessage
}

func NewSimpleInMemoryBroker() *SimpleInMemoryBroker {
	return &SimpleInMemoryBroker{
		queue:     make(chan *SimpleInMemoryBrokerMessage, 100),
		inFlights: make(chan *SimpleInMemoryBrokerMessage, 100),
		expired:   make(chan *SimpleInMemoryBrokerMessage, 100),
	}
}

func (b *SimpleInMemoryBroker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-b.inFlights:
			go func() {
				// log.Println("Message inflight", msg.MessageId, msg.deleted)
				<-msg.enqueueAfter
				if msg.deleted {
					// log.Println("Message deleted", msg.MessageId)
					return
				}

				// log.Println("Message not delete", msg.MessageId)
				b.expired <- msg
			}()
		}
	}
}

func (b *SimpleInMemoryBroker) AddMessages(msgs []*SimpleInMemoryBrokerMessage) {
	for _, msg := range msgs {
		b.queue <- msg
	}
}

func (b *SimpleInMemoryBroker) DeleteMessages(msgs []messages.Message) error {
	for _, msg := range msgs {
		msg.Content().(*SimpleInMemoryBrokerMessage).deleted = true
	}

	return nil
}

func (b *SimpleInMemoryBroker) ReceiveMessages() ([]messages.Message, error) {
	var polledMessage *SimpleInMemoryBrokerMessage
	select {
	case polledMessage = <-b.expired:
	default:
		timer := time.NewTimer(time.Millisecond * 500)
		defer timer.Stop()

		select {
		case <-timer.C:
			return nil, nil
		case polledMessage = <-b.expired:
		case polledMessage = <-b.queue:
		}
	}

	<-time.After(time.Millisecond * 50)

	polledMessage.enqueueAfter = time.After(time.Second * 5)

	msg := messages.Message{
		MsgId:        polledMessage.MessageId,
		Msg:          polledMessage,
		ReceivedTime: time.Now(),
	}

	// Set a context with timeout
	msg.Ctx, msg.CancelCtx = context.WithTimeout(context.Background(), 10*time.Second)

	return []messages.Message{msg}, nil
}
