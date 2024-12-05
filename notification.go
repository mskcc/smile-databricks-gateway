package smile_databricks_gateway

import (
	"context"
	"net/http"
	"strings"
	"time"
)

func NotifyViaSlack(ctx context.Context, body, slackURL string) error {

	slackCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(slackCtx, http.MethodPost, slackURL, strings.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return err
	}
	return nil
}
