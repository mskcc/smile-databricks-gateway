package smile_databricks_gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	st "github.mskcc.org/cdsi/cdsi-protobuf/smile/generated/v1/go"
)

type AWSS3Service struct {
	saml2AWSBin     string
	samlProfile     string
	samlRegion      string
	sessionStart    time.Time
	sessionDuration float64
	client          *s3.Client
}

func NewAWSS3Service(saml2awsBin, samlProfile, samlRegion string, sessionDuration float64) *AWSS3Service {
	return &AWSS3Service{saml2AWSBin: saml2awsBin, samlProfile: samlProfile, samlRegion: samlRegion, sessionDuration: sessionDuration}
}

func (a *AWSS3Service) PutRequest(bucketKey, bucketName string, sr SmileRequest) error {
	s3Client, err := a.getClient()
	if err != nil {
		return fmt.Errorf("Failed to get s3 client: '%s': %q", sr.IgoRequestID, err)
	}
	err = put[SmileRequest](s3Client, bucketKey, bucketName, sr)
	if err != nil {
		return fmt.Errorf("Failed to PutRequest: '%s': %q", sr.IgoRequestID, err)
	}
	return nil
}

func (a *AWSS3Service) PutIGOSample(bucketKey, bucketName string, ss SmileSample) error {
	s3Client, err := a.getClient()
	if err != nil {
		return fmt.Errorf("Failed to get s3 client: '%s': %q", ss.SampleName, err)
	}
	err = put[SmileSample](s3Client, bucketKey, bucketName, ss)
	if err != nil {
		return fmt.Errorf("Failed to PutSample: '%s': %q", ss.SampleName, err)
	}
	return nil
}

func (a *AWSS3Service) PutTEMPOSample(bucketKey, bucketName string, ts st.TempoSample) error {
	s3Client, err := a.getClient()
	if err != nil {
		return fmt.Errorf("Failed to get s3 client: '%s': %q", ts.PrimaryId, err)
	}
	err = put[st.TempoSample](s3Client, bucketKey, bucketName, ts)
	if err != nil {
		return fmt.Errorf("Failed to PutSample: '%s': %q", ts.PrimaryId, err)
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

func (a *AWSS3Service) DeleteObject(bucketKey, bucketName string) error {
	s3Client, err := a.getClient()
	if err != nil {
		return fmt.Errorf("Failed to create S3 client %s:%s: %q", bucketName, bucketKey, err)
	}
	input := &s3.DeleteObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(bucketKey),
	}

	_, err = s3Client.DeleteObject(context.TODO(), input)
	if err != nil {
		return fmt.Errorf("Failed to delete object, %v", err)
	}
	return nil
}

func (a *AWSS3Service) GetRequestObject(bucketKey, bucketName string) (SmileRequest, error) {
	var sr SmileRequest
	data, err := a.getObjectData(bucketKey, bucketName)
	if err != nil {
		return sr, fmt.Errorf("Failed to read object %s:%s: %v", bucketName, bucketKey, err)
	}

	sr, err = UnmarshalT[SmileRequest](data)
	if err != nil {
		return sr, fmt.Errorf("Failed to unmarshal object %s:%s: %v", bucketName, bucketKey, err)
	}

	return sr, nil
}

func (a *AWSS3Service) GetSampleObject(bucketKey, bucketName string) (SmileSample, error) {
	var ss SmileSample
	data, err := a.getObjectData(bucketKey, bucketName)
	if err != nil {
		return ss, fmt.Errorf("Failed to read object %s:%s: %v", bucketName, bucketKey, err)
	}

	ss, err = UnmarshalT[SmileSample](data)
	if err != nil {
		return ss, fmt.Errorf("Failed to unmarshal object %s:%s: %v", bucketName, bucketKey, err)
	}

	return ss, nil
}

func (a *AWSS3Service) getObjectData(bucketKey, bucketName string) ([]byte, error) {
	s3Client, err := a.getClient()
	if err != nil {
		return nil, fmt.Errorf("Failed to create S3 client %s:%s: %q", bucketName, bucketKey, err)
	}
	input := &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(bucketKey),
	}
	output, err := s3Client.GetObject(context.TODO(), input)
	if err != nil {
		return nil, fmt.Errorf("Failed to get object %s:%s: %v", bucketName, bucketKey, err)
	}
	defer output.Body.Close()
	data, err := io.ReadAll(output.Body)
	if err != nil {
		return nil, fmt.Errorf("Failed to read object %s:%s: %v", bucketName, bucketKey, err)
	}

	return data, nil
}

func generateToken(saml2awsBin string) error {
	cmd := exec.Command("sh", saml2awsBin)
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

func (a *AWSS3Service) getClient() (*s3.Client, error) {
	if a.client == nil || a.sessionIsExpired() {
		err := generateToken(a.saml2AWSBin)
		if err != nil {
			return nil, fmt.Errorf("Failed to generate AWS token: %q", err)
		}
		// saml2AWS returns without error, but without being fully setup, lets pause
		time.Sleep(time.Minute)
		s3Client, err := createClient(a.samlProfile, a.samlRegion)
		if err != nil {
			return nil, fmt.Errorf("Failed to create S3 client: %q", err)
		}

		a.sessionStart = time.Now()
		a.client = s3Client
	}
	return a.client, nil
}

func (a *AWSS3Service) sessionIsExpired() bool {
	if time.Since(a.sessionStart).Seconds() < a.sessionDuration {
		return false
	}
	return true
}
