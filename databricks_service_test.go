package smile_databricks_gateway

import (
	"reflect"
	"testing"
)

func TestDatabricksService(t *testing.T) {
	databricksService, close, err := NewDatabricksService(TestConfig.DBToken, TestConfig.DBTokenComment, TestConfig.DBHostname, TestConfig.HttpPath, TestConfig.SMILESchema, TestConfig.RequestTable, TestConfig.SampleTable, TestConfig.SlackURL, TestConfig.DBPort)
	if err != nil {
		t.Fatalf("databricks service cannot be created: %q", err)
	}
	defer close()

	t.Run("inserting request to databricks", func(t *testing.T) {
		wantRequest, err := UnmarshalT[SmileRequest]([]byte(RequestJSON))
		if err != nil {
			t.Fatalf("cannot unmarshal request: %q", err)
		}
		err = databricksService.InsertRequest(wantRequest)
		if err != nil {
			t.Error(err)
		}
		gotRequest, err := databricksService.GetRequest(wantRequest.IgoRequestID)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(gotRequest, wantRequest) {
			t.Errorf("got %v want %v", gotRequest, wantRequest)
		}
		err = databricksService.RemoveRequest("IGO_TEST_REQUEST")
		if err != nil {
			t.Fatalf("error encountered removing request: %q", err)
		}
	})

	t.Run("updating request in databricks", func(t *testing.T) {
		insertedRequest, err := UnmarshalT[SmileRequest]([]byte(RequestJSON))
		if err != nil {
			t.Fatalf("cannot unmarshal request: %q", err)
		}
		err = databricksService.InsertRequest(insertedRequest)
		if err != nil {
			t.Error(err)
		}
		updatedRequest, err := UnmarshalT[SmileRequest]([]byte(RequestJSON))
		if err != nil {
			t.Fatalf("cannot unmarshal request: %q", err)
		}
		updatedRequest.LabHeadName = "homer simpson"
		err = databricksService.UpdateRequest(updatedRequest)
		if err != nil {
			t.Error(err)
		}
		gotRequest, err := databricksService.GetRequest(updatedRequest.IgoRequestID)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(gotRequest, updatedRequest) {
			t.Errorf("got %v want %v", gotRequest, updatedRequest)
		}

		err = databricksService.RemoveRequest("IGO_TEST_REQUEST")
		if err != nil {
			t.Fatalf("error encountered removing request: %q", err)
		}
	})

	t.Run("inserting sample to databricks", func(t *testing.T) {
		wantRequest, err := UnmarshalT[SmileRequest]([]byte(RequestJSON))
		if err != nil {
			t.Fatalf("cannot unmarshal request: %q", err)
		}
		err = databricksService.InsertSamples(wantRequest.IgoRequestID, wantRequest.Samples)
		if err != nil {
			t.Error(err)
		}
		wantSample := wantRequest.Samples[0]
		gotSample, err := databricksService.GetSample(wantRequest.IgoRequestID, wantSample.SampleName)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(gotSample, wantSample) {
			t.Errorf("got %v want %v", gotSample, wantSample)
		}
		err = databricksService.RemoveSamples("IGO_TEST_REQUEST")
		if err != nil {
			t.Fatalf("error encountered removing sample: %q", err)
		}
	})

	t.Run("updating sample to databricks", func(t *testing.T) {
		wantRequest, err := UnmarshalT[SmileRequest]([]byte(RequestJSON))
		if err != nil {
			t.Fatalf("cannot unmarshal request: %q", err)
		}
		err = databricksService.InsertSamples(wantRequest.IgoRequestID, wantRequest.Samples)
		if err != nil {
			t.Error(err)
		}
		updatedSample := wantRequest.Samples[0]
		updatedSample.CmoSampleName = "brooklyn bonnies"
		err = databricksService.UpdateSample(wantRequest.IgoRequestID, updatedSample)
		if err != nil {
			t.Error(err)
		}
		gotSample, err := databricksService.GetSample(wantRequest.IgoRequestID, updatedSample.SampleName)
		if err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(gotSample, updatedSample) {
			t.Errorf("got %v want %v", gotSample, updatedSample)
		}
		err = databricksService.RemoveSamples("IGO_TEST_REQUEST")
		if err != nil {
			t.Fatalf("error encountered removing sample: %q", err)
		}
	})

}
