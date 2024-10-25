package smile_databricks_gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/files"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
)

type DatabricksRestService struct {
	wClient          *databricks.WorkspaceClient
	dbfsPath         string
	smileDLTPipeName string
}

func NewDatabricksRestService(dbInstance, token, dbfsPath, smileDLTPipeName string) (*DatabricksRestService, error) {
	w, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("https://%s", dbInstance),
		Token:       token,
		Credentials: config.PatCredentials{},
	})
	if err != nil {
		return nil, fmt.Errorf("Cannot create a databricks workspace client: %v", err)
	}
	return &DatabricksRestService{wClient: w, dbfsPath: dbfsPath, smileDLTPipeName: smileDLTPipeName}, nil
}

func (d *DatabricksRestService) PutRequest(key string, sr SmileRequest) error {
	rJson, err := json.Marshal(sr)
	if err != nil {
		return fmt.Errorf("Failed to marshal request: '%s': %q", sr.IgoRequestID, err)
	}
	filePath := fmt.Sprintf("%s/%s", d.dbfsPath, key)
	uploadReq := files.UploadRequest{
		FilePath:  filePath,
		Contents:  io.NopCloser(strings.NewReader(string(rJson))),
		Overwrite: true,
	}
	err = d.wClient.Files.Upload(context.Background(), uploadReq)
	if err != nil {
		return fmt.Errorf("Failed to upload file: '%s': %q", sr.IgoRequestID, err)
	}
	return nil
}

func (d *DatabricksRestService) ExecutePipeline(ctx context.Context) error {
	pipelineId, err := d.getPipelineIdByName(ctx)
	if err != nil {
		return fmt.Errorf("Failed to get SMILE DLT Pipeline id: '%s': %q", d.smileDLTPipeName, err)
	}

	if _, err = d.wClient.Pipelines.WaitGetPipelineIdle(ctx, pipelineId, 15*time.Minute, nil); err != nil {
		return fmt.Errorf("Error waiting for pipeline to get in idle state: '%s': %q", d.smileDLTPipeName, err)
	}

	_, err = d.wClient.Pipelines.StartUpdate(ctx, pipelines.StartUpdate{PipelineId: pipelineId})
	if err != nil {
		return fmt.Errorf("Failed to run SMILE DLT Pipeline id: '%s': %q", d.smileDLTPipeName, err)
	}

	if _, err := d.wClient.Pipelines.WaitGetPipelineRunning(ctx, pipelineId, 15*time.Minute, nil); err != nil {
		return fmt.Errorf("Error waiting for pipeline to start running: '%s': %q", d.smileDLTPipeName, err)
	}

	return nil
}

func (d *DatabricksRestService) getPipelineIdByName(ctx context.Context) (string, error) {
	pipelines, err := d.wClient.Pipelines.ListPipelinesAll(ctx, pipelines.ListPipelinesRequest{})
	if err != nil {
		return "", err
	}
	for _, pipeline := range pipelines {
		if pipeline.Name == d.smileDLTPipeName {
			return pipeline.PipelineId, nil
		}
	}
	return "", fmt.Errorf("pipeline '%s' not found", d.smileDLTPipeName)
}
