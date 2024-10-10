package smile_databricks_gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"

	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/config"
	"github.com/databricks/databricks-sdk-go/service/files"
)

type DatabricksRestService struct {
	wClient  *databricks.WorkspaceClient
	dbfsPath string
}

func NewDatabricksRestService(dbInstance, token, dbfsPath string) (*DatabricksRestService, error) {
	w, err := databricks.NewWorkspaceClient(&databricks.Config{
		Host:        fmt.Sprintf("https://%s", dbInstance),
		Token:       token,
		Credentials: config.PatCredentials{},
	})
	if err != nil {
		return nil, fmt.Errorf("Cannot create a databricks workspace client: %v", err)
	}
	return &DatabricksRestService{wClient: w, dbfsPath: dbfsPath}, nil
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
