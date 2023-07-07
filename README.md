# go-queue-worker

This Go Module implements a queue worker.

## Supported clients

The module supports SQS queue out of the box but you can pass any struct implementing the interface Client.

## Basic usage (SQS)

```go
import (
    "context"
    "fmt"
    "log"

    "github.com/francescopepe/go-queue-worker"
    workerSqs "github.com/francescopepe/go-queue-worker/clients/sqs"
    
    "github.com/aws/aws-sdk-go-v2/aws"
    "github.com/aws/aws-sdk-go-v2/config"
    "github.com/aws/aws-sdk-go-v2/service/sqs"
)

func main() {
    ctx := context.Background()

    awsCfg, err := config.LoadDefaultConfig(ctx)
    if err != nil {
        log.Fatalln("Unable to create AWS config", err)
    }
    
    sqsSvc := sqs.NewFromConfig(awsCfg)
    
    wkr := worker.NewWorker(worker.Configuration{
        Client: workerSqs.NewSqsClient(workerSqs.SqsClientConfiguration{
            Svc: sqsSvc,
            ReceiveMessageInput: &sqs.ReceiveMessageInput{
                QueueUrl:            aws.String(os.Getenv("SQS_QUEUE_URL")),
                MaxNumberOfMessages: 10,
                VisibilityTimeout:   30,
                WaitTimeSeconds:     20,
            },
        }),
        Concurrency: 100,
        Retrievers:  2,
        ErrorConfig: worker.ErrorConfiguration{
            Threshold: 5,
            Period:    time.Minute * 1,
            ReportFunc: func(err error) {
                log.Println("Worker error", err)
            },
        },
        Consumer: worker.NewMultiMessageConsumer(worker.MultiMessageConsumerConfiguration{
            BufferConfig: worker.MultiMessageBufferConfiguration{
            Size:    500,
			Timeout: time.Second * 1,
            },
            Handler: func(ctx context.Context, msgs []interface{}) error {
                // Your logic here...


                log.Println("Got batch", len(msgs))
				
                // Assert the type of message to get the body or any other attributes 
                log.Println("First message body", *msgs[0].(types.Message).Body)
    
                return nil
            },
        }),
    })
    
    err = wkr.Run(ctx)
    if err != nil {
        log.Fataln("Worker stopped with error", err)
    }
}
```