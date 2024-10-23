package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	ddlambda "github.com/DataDog/datadog-lambda-go"
	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func init() {
	cfg, err := config.LoadDefaultConfig(context.TODO())
	if err != nil {
		log.Fatalf("Failed to load AWS config: %v", err)
	}
	_ = sqs.NewFromConfig(cfg)
}

func main() {
	lambda.Start(ddlambda.WrapFunction(FunctionHandler, nil))
}

func FunctionHandler(ctx context.Context, event json.RawMessage) (string, error) {
	var processedMessages int

	// Try to unmarshal as SQS event
	var sqsEvent events.SQSEvent
	if err := json.Unmarshal(event, &sqsEvent); err == nil && len(sqsEvent.Records) > 0 {
		count, err := processSQSEvent(ctx, sqsEvent)
		if err != nil {
			return "", err
		}
		processedMessages += count
	} else {
		// Try to unmarshal as SNS event
		var snsEvent events.SNSEvent
		if err := json.Unmarshal(event, &snsEvent); err == nil && len(snsEvent.Records) > 0 {
			count, err := processSNSEvent(ctx, snsEvent)
			if err != nil {
				return "", err
			}
			processedMessages += count
		} else {
			// Assume it's an EventBridge event
			count, err := processEventBridgeEvent(ctx, event)
			if err != nil {
				return "", err
			}
			processedMessages += count
		}
	}

	return fmt.Sprintf("Processed %d messages", processedMessages), nil
}

func processSQSEvent(ctx context.Context, event events.SQSEvent) (int, error) {
	processedCount := 0
	for _, record := range event.Records {
		log.Printf("Received SQS message: %s", record.Body)
		fmt.Println("Hello World from SQS")
		processedCount++
	}
	return processedCount, nil
}

func processSNSEvent(ctx context.Context, event events.SNSEvent) (int, error) {
	processedCount := 0
	for _, record := range event.Records {
		log.Printf("Received SNS message: %s", record.SNS.Message)
		fmt.Println("Hello World from SNS")
		processedCount++
	}
	return processedCount, nil
}

func processEventBridgeEvent(ctx context.Context, event json.RawMessage) (int, error) {
	log.Printf("Received EventBridge event: %s", string(event))
	fmt.Println("Hello World from EventBridge")
	return 1, nil
}
