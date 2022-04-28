package worker

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
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

func getQueueURL(client QueueAPI, queueName string) (queueURL string) {
	params := &sqs.GetQueueUrlInput{
		QueueName: aws.String(queueName), // Required
	}
	response, err := client.GetQueueUrl(params)
	if err != nil {
		fmt.Println(err.Error())
		return
	}
	queueURL = aws.ToString(response.QueueUrl)

	return
}
