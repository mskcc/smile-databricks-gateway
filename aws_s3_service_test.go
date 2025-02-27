package smile_databricks_gateway

import (
	"fmt"
	"reflect"
	"testing"
)

func TestAWSS3(t *testing.T) {

	awsS3Service := NewAWSS3Service(TestConfig.SAML2AWSBin, TestConfig.SAMLProfile, TestConfig.SAMLRegion, TestConfig.AWSSessionDuration)

	t.Run("PutRequest", func(t *testing.T) {
		putRequest, err := UnmarshalT[SmileRequest]([]byte(RequestJSON))
		if err != nil {
			t.Fatalf("cannot unmarshal request: %q", err)
		}
		filename := fmt.Sprintf("%s_request.json", putRequest.IgoRequestID)
		err = awsS3Service.PutRequest(filename, TestConfig.IGOAWSBucket, putRequest)
		if err != nil {
			t.Fatalf("cannot PutRequest: %q", err)
		}
		gotRequest, err := awsS3Service.GetRequestObject(filename, TestConfig.IGOAWSBucket)
		if err != nil {
			t.Fatalf("cannot GetRequest: %q", err)
		}
		if !reflect.DeepEqual(gotRequest, putRequest) {
			t.Errorf("got %v want %v", gotRequest, putRequest)
		}
		err = awsS3Service.DeleteObject(filename, TestConfig.IGOAWSBucket)
		if err != nil {
			t.Fatalf("cannot DeleteObject: %q", err)
		}
	})

	t.Run("PutSample", func(t *testing.T) {
		putRequest, err := UnmarshalT[SmileRequest]([]byte(RequestJSON))
		if err != nil {
			t.Fatalf("cannot unmarshal request: %q", err)
		}
		putSample := putRequest.Samples[0]
		filename := fmt.Sprintf("%s_sample.json", putSample.SampleName)
		err = awsS3Service.PutSample(filename, TestConfig.IGOAWSBucket, putSample)
		if err != nil {
			t.Fatalf("cannot PutSample: %q", err)
		}
		gotSample, err := awsS3Service.GetSampleObject(filename, TestConfig.IGOAWSBucket)
		if err != nil {
			t.Fatalf("cannot GetSample: %q", err)
		}
		if !reflect.DeepEqual(gotSample, putSample) {
			t.Errorf("got %v want %v", gotSample, putSample)
		}
		err = awsS3Service.DeleteObject(filename, TestConfig.IGOAWSBucket)
		if err != nil {
			t.Fatalf("cannot DeleteObject: %q", err)
		}
	})
}
