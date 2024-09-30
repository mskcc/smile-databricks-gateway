package smile_databricks_gateway

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

const (
	region = "us-east-1"
)

func GenerateToken(saml2awsCMD string) error {
	cmd := exec.Command("bash", saml2awsCMD)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("Failed to run %q, err: %v", saml2awsCMD, err)
	}
	return nil
}

func CreateClient(credsProfile, region string) (*s3.Client, error) {
	var client *s3.Client
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithSharedConfigProfile(credsProfile))
	if err != nil {
		return client, fmt.Errorf("Failed to load SDK configuration: %v", err)
	}
	return s3.NewFromConfig(cfg), nil
}

func PutObject(client *s3.Client, content []byte, bucketKey, bucketName string) error {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(bucketKey),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("application/json"),
	}

	_, err := client.PutObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to upload object, %v", err)
	}

	return nil
}

func DeleteObject(client *s3.Client, bucketKey, bucketName string) error {
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(bucketKey),
	}

	_, err := client.DeleteObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("failed to delete object, %v", err)
	}
	return nil
}
