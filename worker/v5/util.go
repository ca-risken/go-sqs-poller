package worker

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func (config *Config) populateDefaultValues() {
	if config.MaxNumberOfMessage == 0 {
		config.MaxNumberOfMessage = 10
	}

	if config.WaitTimeSecond == 0 {
		config.WaitTimeSecond = 20
	}
}

func getQueueURL(ctx context.Context, client QueueAPI, queueName string) (queueURL string) {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName), // Required
	}
	response, err := client.GetQueueUrl(ctx, params)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	queueURL = aws.ToString(response.QueueUrl)

	return
}

func CreateSqsClient(ctx context.Context, region, sqsEndpoint string) (QueueDeleteReceiverAPI, error) {
	customResolver := aws.EndpointResolverWithOptionsFunc(func(service, region string, options ...interface{}) (aws.Endpoint, error) {
		if service != sqs.ServiceID {
			// returning EndpointNotFoundError will allow the service to fallback to it's default resolution
			return aws.Endpoint{}, &aws.EndpointNotFoundError{}
		}

		if sqsEndpoint != "" {
			return aws.Endpoint{
				PartitionID:   "aws",
				URL:           sqsEndpoint,
				SigningRegion: region,
			}, nil
		}
		return aws.Endpoint{
			PartitionID:   "aws",
			SigningRegion: region,
		}, nil
	})
	cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithEndpointResolverWithOptions(customResolver))
	if err != nil {
		return nil, fmt.Errorf("Failed to load aws configuration, err=%+w", err)
	}
	return sqs.NewFromConfig(cfg), nil
}
