package clients

import (
	"context"
	"fmt"
	"time"

	awsSqs "github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/francescopepe/go-queue-worker/internal/client"
	"github.com/francescopepe/go-queue-worker/internal/messages"
)

type SqsClientConfiguration struct {
	// The AWS Sqs Service Client
	Svc *awsSqs.Client

	// The AWS ReceiveMessageInput
	ReceiveMessageInput *awsSqs.ReceiveMessageInput

	// Defines the interval within which the message must be processed.
	// If empty, it tries to set the value from the ReceiveMessageInput's
	// VisibilityTimeout.
	// If not defined, the messages' context will never expire.
	// It's highly recommended to set this value.
	MessageCtxTimeout time.Duration
}

type sqsClient struct {
	svc                 *awsSqs.Client
	receiveMessageInput *awsSqs.ReceiveMessageInput
	messageCtxTimeout   time.Duration
}

func (c sqsClient) ReceiveMessages() ([]messages.Message, error) {
	out, err := c.svc.ReceiveMessage(context.Background(), c.receiveMessageInput)
	if err != nil {
		return nil, fmt.Errorf("unable to receive messages: %w", err)
	}

	msgs := make([]messages.Message, len(out.Messages))
	for i, msg := range out.Messages {
		msgs[i] = c.createMessage(msg)
	}

	return msgs, nil
}

func (c sqsClient) DeleteMessages(messages []messages.Message) error {
	deleteEntries := make([]types.DeleteMessageBatchRequestEntry, 0, len(messages))
	for _, message := range messages {
		deleteEntries = append(deleteEntries, types.DeleteMessageBatchRequestEntry{
			Id:            message.Msg.(types.Message).MessageId,
			ReceiptHandle: message.Msg.(types.Message).ReceiptHandle,
		})
	}

	_, err := c.svc.DeleteMessageBatch(context.Background(), &awsSqs.DeleteMessageBatchInput{
		Entries:  deleteEntries,
		QueueUrl: c.receiveMessageInput.QueueUrl,
	})

	return err
}

func (c sqsClient) createMessage(sqsMessage types.Message) messages.Message {
	msg := messages.Message{
		Msg: sqsMessage,
	}

	timeout := c.messageCtxTimeout
	if timeout == 0 {
		// Try to infer from ReceiveMessage's VisibilityTimeout
		timeout = time.Second * time.Duration(c.receiveMessageInput.VisibilityTimeout)
	}

	if timeout == 0 {
		// Set a context that never expires
		msg.Ctx, msg.CancelCtx = context.Background(), func() {}

		return msg
	}

	// Set a context with timeout
	msg.Ctx, msg.CancelCtx = context.WithTimeout(context.Background(), timeout)

	return msg
}

func NewSqsClient(config SqsClientConfiguration) sqsClient {
	return sqsClient{
		svc:                 config.Svc,
		receiveMessageInput: config.ReceiveMessageInput,
		messageCtxTimeout:   config.MessageCtxTimeout,
	}
}

// Interface guards
var (
	_ client.Client = (*sqsClient)(nil)
)
