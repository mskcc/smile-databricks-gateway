package smile_databricks_gateway

import (
	"context"
	"fmt"
	"time"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/pipelines"
)

type DatabricksService struct {
	wClient          *databricks.WorkspaceClient
	smileDLTPipeName string
}

func NewDatabricksService(dbInstance, token, smileDLTPipeName string) (*DatabricksService, error) {
	w, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("https://%s", dbInstance),
		Token:       token,
		Credentials: config.PatCredentials{},
	})
	if err != nil {
		return nil, fmt.Errorf("Cannot create a databricks workspace client: %v", err)
	}
	return &DatabricksService{wClient: w, smileDLTPipeName: smileDLTPipeName}, nil
}

func (d *DatabricksService) ExecutePipeline(ctx context.Context) error {
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

func (d *DatabricksService) getPipelineIdByName(ctx context.Context) (string, error) {
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
