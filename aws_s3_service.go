package smile_databricks_gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type AWSS3Service struct {
	saml2AWSBin string
	samlProfile string
	samlRegion  string
	bucketName  string
}

func NewAWSS3Service(saml2awsBin, samlProfile, samlRegion, bucketName string) *AWSS3Service {
	return &AWSS3Service{saml2AWSBin: saml2awsBin, samlProfile: samlProfile, samlRegion: samlRegion, bucketName: bucketName}
}

func (a *AWSS3Service) PutRequest(bucketKey string, sr SmileRequest) error {
	s3Client, err := a.getClient()
	if err != nil {
		return fmt.Errorf("Failed to get s3 client: '%s': %q", sr.IgoRequestID, err)
	}
	err = put[SmileRequest](s3Client, bucketKey, a.bucketName, sr)
	if err != nil {
		return fmt.Errorf("Failed to PutRequest: '%s': %q", sr.IgoRequestID, err)
	}
	return nil
}

func (a *AWSS3Service) PutSample(bucketKey string, ss SmileSample) error {
	s3Client, err := a.getClient()
	if err != nil {
		return fmt.Errorf("Failed to get s3 client: '%s': %q", ss.SampleName, err)
	}
	err = put[SmileSample](s3Client, bucketKey, a.bucketName, ss)
	if err != nil {
		return fmt.Errorf("Failed to PutSample: '%s': %q", ss.SampleName, err)
	}
	return nil
}

func put[T any](s3Client *s3.Client, awsBucketKey, awsDestBucket string, t T) error {
	rJson, err := json.Marshal(t)
	if err != nil {
		return fmt.Errorf("Failed to marshal: %q", err)
	}

	err = putObject(s3Client, []byte(rJson), awsBucketKey, awsDestBucket)
	if err != nil {
		return fmt.Errorf("Failed to putObject: %q", err)
	}
	return nil
}

func generateToken(saml2awsBin string) error {
	cmd := exec.Command("bash", saml2awsBin)
	err := cmd.Run()
	if err != nil {
		return fmt.Errorf("Failed to run %q, err: %v", saml2awsBin, err)
	}
	return nil
}

func createClient(credsProfile, region string) (*s3.Client, error) {
	var client *s3.Client
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion(region),
		config.WithSharedConfigProfile(credsProfile))
	if err != nil {
		return client, fmt.Errorf("Failed to load SDK configuration: %v", err)
	}
	return s3.NewFromConfig(cfg), nil
}

func putObject(client *s3.Client, content []byte, bucketKey, bucketName string) error {
	input := &s3.PutObjectInput{
		Bucket:      aws.String(bucketName),
		Key:         aws.String(bucketKey),
		Body:        bytes.NewReader(content),
		ContentType: aws.String("application/json"),
	}

	_, err := client.PutObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("Failed to upload object, %v", err)
	}

	return nil
}

func (a *AWSS3Service) DeleteObject(bucketKey string) error {
	s3Client, err := a.getClient()
	if err != nil {
		return fmt.Errorf("Failed to create S3 client %s:%s: %q", a.bucketName, bucketKey, err)
	}
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(a.bucketName),
		Key:    aws.String(bucketKey),
	}

	_, err = s3Client.DeleteObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("Failed to delete object, %v", err)
	}
	return nil
}

func (a *AWSS3Service) GetRequestObject(bucketKey string) (SmileRequest, error) {
	var sr SmileRequest
	data, err := a.getObjectData(bucketKey)
	if err != nil {
		return sr, fmt.Errorf("Failed to read object %s:%s: %v", a.bucketName, bucketKey, err)
	}

	sr, err = UnmarshalT[SmileRequest](data)
	if err != nil {
		return sr, fmt.Errorf("Failed to unmarshal object %s:%s: %v", a.bucketName, bucketKey, err)
	}

	return sr, nil
}

func (a *AWSS3Service) GetSampleObject(bucketKey string) (SmileSample, error) {
	var ss SmileSample
	data, err := a.getObjectData(bucketKey)
	if err != nil {
		return ss, fmt.Errorf("Failed to read object %s:%s: %v", a.bucketName, bucketKey, err)
	}

	ss, err = UnmarshalT[SmileSample](data)
	if err != nil {
		return ss, fmt.Errorf("Failed to unmarshal object %s:%s: %v", a.bucketName, bucketKey, err)
	}

	return ss, nil
}

func (a *AWSS3Service) getObjectData(bucketKey string) ([]byte, error) {
	s3Client, err := a.getClient()
	if err != nil {
		return nil, fmt.Errorf("Failed to create S3 client %s:%s: %q", a.bucketName, bucketKey, err)
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(a.bucketName),
		Key:    aws.String(bucketKey),
	}
	output, err := s3Client.GetObject(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("Failed to get object %s:%s: %v", a.bucketName, bucketKey, err)
	}
	defer output.Body.Close()
	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read object %s:%s: %v", a.bucketName, bucketKey, err)
	}

	return data, nil
}

func (a *AWSS3Service) getClient() (*s3.Client, error) {
	err := generateToken(a.saml2AWSBin)
	if err != nil {
		return nil, fmt.Errorf("Failed to generate AWS token: %q", err)
	}

	s3Client, err := createClient(a.samlProfile, a.samlRegion)
	if err != nil {
		return nil, fmt.Errorf("Failed to create S3 client: %q", err)
	}
	return s3Client, nil
}
