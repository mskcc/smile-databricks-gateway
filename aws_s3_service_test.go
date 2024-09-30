package smile_databricks_gateway

import (
	"testing"
)

func TestAWSS3(t *testing.T) {

	t.Run("generating saml2aws token", func(t *testing.T) {
		err := GenerateToken(TestConfig.SAML2AWSBin)
		if err != nil {
			t.Fatalf("cannot generate saml2aws token: %q", err)
		}
	})

	t.Run("put object without saml2aws token", func(t *testing.T) {
		wantErr := "Failed to load SDK configuration: failed to get shared config profile, bogus profile"
		_, gotErr := CreateClient("bogus profile", "bogus region")
		if gotErr.Error() != wantErr {
			t.Fatalf("got %q want %q", gotErr, wantErr)
		}
	})

	t.Run("put object with saml2aws token", func(t *testing.T) {
		err := GenerateToken(TestConfig.SAML2AWSBin)
		if err != nil {
			t.Fatalf("cannot generate saml2aws token: %q", err)
		}
		s3Client, err := CreateClient("saml", "us-east-1")
		if err != nil {
			t.Fatalf("cannot create s3Client: %q", err)
		}

		bucketName := "cbio-databucket"
		objectKey := "test-file.txt"
		fileContents := []byte("This is the file contents")
		err = PutObject(s3Client, fileContents, objectKey, bucketName)
		if err != nil {
			t.Fatalf("cannot put test object into s3 bucket: %q", err)
		}
		err = DeleteObject(s3Client, objectKey, bucketName)
		if err != nil {
			t.Fatalf("cannot delete test object in s3 bucket: %q", err)
		}
	})
}
