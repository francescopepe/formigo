# Go queue worker - A Golang Library for Efficient Queue Processing.

Go queue worker is a powerful and flexible Golang library designed to simplify the processing of messages from queues. It currently supports AWS SQS, with the capability to extend its functionality to accommodate multiple types of queues. With this library, you can effortlessly manage and scale the concurrent processing of messages, ensuring efficient utilization of resources and increased throughput.

## Key Features

- **Efficient Throughput Management**: it offers optimal throughput management, allowing you to fine-tune the number of Go routines responsible for both polling messages from the queue and processing them. This dynamic control ensures maximum efficiency in various scenarios, making the library highly adaptable to your application's needs.

- **Configurable Batch Processing**: it uses powerful batch processing capabilities, enabling you to handle messages efficiently in customizable batches. With the Multiple Message Handler, messages can be processed in batches of a size you define, granting you full control over the processing logic. Moreover, you can adjust the batch buffer size and timeout settings, providing a flexible and optimal solution to process messages under various workloads.

- **Context Cancellation**: Effortlessly stop the QueueWorker by canceling its context. This feature guarantees smooth and controlled termination of the worker whenever required.

- **Custom Error Reporting**: Define a custom reporting function to receive and manage any errors that occur during message processing. This flexibility enables seamless integration with your existing error reporting mechanisms.

- **Error Threshold Management**: Set the worker to stop automatically if a certain number of errors (X) occur within a specific interval (T). This proactive approach ensures the system's stability and helps prevent cascading failures.

- **Message context timeout**: Each message or batch is associated with a context that expires within its visibility timeout. The handler must process the message or batch within this visibility timeout to prevent re-processing by other workers, ensuring reliable message handling.

## Installation

Make sure you have Go installed (download)[https://go.dev/dl/]

Initialize your project by creating a folder and then running `go mod init github.com/your/repo` inside the folder. Then install the library with the (`go get`)[https://pkg.go.dev/cmd/go/#hdr-Add_dependencies_to_current_module_and_install_them] command:

```bash
go get -u github.com/francescopepe/go-queue-worker
```

## Examples

Let's create some simple examples to demonstrate how to use this library to process messages from an AWS SQS queue.

### Basic example

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
    "github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
        Consumer: worker.NewSingleMessageConsumer(worker.SingleMessageConsumerConfiguration{
            Handler: func(ctx context.Context, msg interface{}) error {
                log.Println("Got Message", msgs)

                // Assert the type of message to get the body or any other attributes
                log.Println("Message body", *msg.(types.Message).Body)

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

In this example, we have created a worker that consumes messages one at a time from an AWS SQS queue. The polling phase retrieves 10 messages from the queue, but the handler processes them individually.

By default, the worker's concurrency is set to 100, meaning it can process up to 100 messages concurrently, optimizing throughput and efficiency.

If any errors occur during message handling, the worker will log them using log.PrintLn by default. Additionally, the worker is configured to stop if it encounters more than 3 errors within any 120-second interval.

Please note that these are the default settings, and you can customize the concurrency level, error handling, and other parameters to suit your specific requirements.


### Batching

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
    "github.com/aws/aws-sdk-go-v2/service/sqs/types"
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
        Consumer: worker.NewMultiMessageConsumer(worker.MultiMessageConsumerConfiguration{
            BufferConfig: worker.MultiMessageBufferConfiguration{
                Size:    100,
                Timeout: time.Second * 5,
            },
            Handler: func(ctx context.Context, msgs []interface{}) error {
                log.Printf("Got %d messages to process\n", len(msgs)

                // Assert the type of message to get the body or any other attributes

                for i, msg := range msgs {
                    log.Printf("Message %d body: %s", i, *msg.(types.Message).Body)
                }

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

In this example, we have created a worker that efficiently consumes batches of messages from an AWS SQS queue. The handler will be invoked either when the buffer is full or when a specified timeout expires.

It's essential to note that the timer starts as soon as the first message is added to the buffer.

By processing messages in batches, the worker can significantly enhance throughput for specific use cases or reduce resource consumption. For instance, it can be leveraged for batch insertions or deletions.

## Configuration

| Configuration | Explanation | Default Value |
|-------------- | ----------- | ------------- |
| Client | The client is used for receiving messages from the queue and deleting them once they are processed correctly. This is a required configuration. | None |
| Concurrency | Number of Go routines that process the messages from the Queue. Higher values are useful for slow I/O operations in the consumer's handler. | 100 |
| Retrievers | Number of Go routines that retrieve messages from the Queue. Higher values are helpful for slow networks or when consumers are quicker. | 1 |
| ErrorConfig | Defines the error threshold and interval for worker termination and error reporting function. | None |
| Consumer | The message consumer, either SingleMessageConsumer or MultipleMessageConsumer. | None |

## License

This library is distributed under the [MIT License](/LICENSE)
