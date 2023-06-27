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

const (
	defaultMaxNumberOfMessages = 10
	defaultWaitTimeSeconds     = 20 // Implements long polling
)

type RetrieveMessageConfiguration struct {
	// The maximum number of messages to return. Amazon SQS never returns more
	// messages than this value (however, fewer messages might be returned). Valid
	// values: 1 to 10. Default: 10.
	MaxNumberOfMessages int32

	// The duration (in seconds) that the received messages are hidden from subsequent
	// retrieve requests after being retrieved by a ReceiveMessage request.
	// This should be lower than the maximum processing time, if not provided the default
	// value on the queue is used.
	VisibilityTimeout int32

	// The duration (in seconds) for which the call waits for a message to arrive in
	// the queue before returning. If a message is available, the call returns sooner
	// than WaitTimeSeconds. If no messages are available and the wait time expires,
	// the call returns successfully with an empty list of messages. To avoid HTTP
	// errors, ensure that the HTTP response timeout for ReceiveMessage requests is
	// longer than the WaitTimeSeconds parameter.
	// Valid values: 0 to 20. Default: 20.
	WaitTimeSeconds int32
}

type SqsClientConfiguration struct {
	// The AWS sqsService
	SqsSvc *awsSqs.Client

	// The URL of the Amazon SQS queue from which messages are received. Queue URLs
	// and names are case-sensitive.
	QueueUrl string

	// RetrieveMessageConfiguration defines the configuration used for retrieving the messages.
	RetrieveMessageConfig RetrieveMessageConfiguration
}

func setDefaultClientConfigValues(config SqsClientConfiguration) SqsClientConfiguration {
	if config.RetrieveMessageConfig.MaxNumberOfMessages == 0 {
		config.RetrieveMessageConfig.MaxNumberOfMessages = defaultMaxNumberOfMessages
	}

	if config.RetrieveMessageConfig.WaitTimeSeconds == 0 {
		config.RetrieveMessageConfig.WaitTimeSeconds = defaultWaitTimeSeconds
	}

	return config
}

type SqsClient struct {
	sqsSvc         *awsSqs.Client
	queueUrl       string
	retrieveConfig RetrieveMessageConfiguration

	messageCtxTimeout time.Duration
}

func (c SqsClient) ReceiveMessages() ([]messages.Message, error) {
	out, err := c.sqsSvc.ReceiveMessage(context.Background(), &awsSqs.ReceiveMessageInput{
		QueueUrl:            &c.queueUrl,
		MaxNumberOfMessages: c.retrieveConfig.MaxNumberOfMessages,
		VisibilityTimeout:   c.retrieveConfig.VisibilityTimeout,
		WaitTimeSeconds:     c.retrieveConfig.WaitTimeSeconds,
		AttributeNames:      []types.QueueAttributeName{types.QueueAttributeNameAll},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to receive messages: %w", err)
	}

	messages := make([]messages.Message, len(out.Messages))
	for i, msg := range out.Messages {
		messages[i].Msg = msg
		messages[i].Ctx, messages[i].CancelCtx = context.WithTimeout(context.Background(), c.messageCtxTimeout)
	}

	return messages, nil
}

func (c SqsClient) DeleteMessages(messages []messages.Message) error {
	deleteEntries := make([]types.DeleteMessageBatchRequestEntry, 0, len(messages))
	for _, message := range messages {
		deleteEntries = append(deleteEntries, types.DeleteMessageBatchRequestEntry{
			Id:            message.Msg.(types.Message).MessageId,
			ReceiptHandle: message.Msg.(types.Message).ReceiptHandle,
		})
	}

	_, err := c.sqsSvc.DeleteMessageBatch(context.Background(), &awsSqs.DeleteMessageBatchInput{
		Entries:  deleteEntries,
		QueueUrl: &c.queueUrl,
	})

	return err
}

func NewSqsClient(config SqsClientConfiguration) SqsClient {
	config = setDefaultClientConfigValues(config)

	return SqsClient{
		sqsSvc:            config.SqsSvc,
		queueUrl:          config.QueueUrl,
		retrieveConfig:    config.RetrieveMessageConfig,
		messageCtxTimeout: time.Second * time.Duration(config.RetrieveMessageConfig.VisibilityTimeout),
	}
}

// Interface guards
var (
	_ client.Client = (*SqsClient)(nil)
)
