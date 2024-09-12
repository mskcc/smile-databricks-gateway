package smile_databricks_gateway

import (
	"database/sql"
	"encoding/json"
	"fmt"

	dbsql "github.com/databricks/databricks-sql-go"
)

type DatabricksService struct {
	tokenComment string
	db           *sql.DB
	schema       string
	requestTable string
	sampleTable  string
}

func NewDatabricksService(token, tokenComment, hostname, httpPath, schema, requestTable, sampleTable, slackURL string, port int) (*DatabricksService, func(), error) {
	db, err := openDatabase(token, hostname, httpPath, port)
	if err != nil {
		return nil, nil, err
	}
	closeFunc := func() {
		db.Close()
	}
	return &DatabricksService{tokenComment: tokenComment, db: db, schema: schema, requestTable: requestTable, sampleTable: sampleTable}, closeFunc, nil
}

func (d *DatabricksService) InsertRequest(sr SmileRequest) error {
	if err := d.db.Ping(); err != nil {
		return fmt.Errorf("Failed to connect to database request: '%s': %q", sr.IgoRequestID, err)
	}

	rJson, err := json.Marshal(sr)
	if err != nil {
		return fmt.Errorf("Failed to marshal request: '%s': %q", sr.IgoRequestID, err)
	}

	query := fmt.Sprintf("insert into %s.%s values ('%s', '%s')", d.schema, d.requestTable, sr.IgoRequestID, rJson)
	res, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to insert request: '%s': %q", sr.IgoRequestID, err)
	}

	var expectedCount int64 = 1
	insertedCount, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("Failed to fetch rows affected request: '%s': %q", sr.IgoRequestID, err)
	}

	if insertedCount != expectedCount {
		return fmt.Errorf("Expected '%d' request rows inserted but got '%d', request: '%s'", expectedCount, insertedCount, sr.IgoRequestID)
	}

	return nil
}

func (d *DatabricksService) UpdateRequest(sr SmileRequest) error {
	if err := d.db.Ping(); err != nil {
		return fmt.Errorf("Failed to connect to database request: '%s': %q", sr.IgoRequestID, err)
	}

	rJson, err := json.Marshal(sr)
	if err != nil {
		return fmt.Errorf("Failed to marshal request: '%s': %q", sr.IgoRequestID, err)
	}

	query := fmt.Sprintf("update %s.%s set REQUEST_JSON = '%s' where IGO_REQUEST_ID = '%s'", d.schema, d.requestTable, rJson, sr.IgoRequestID)
	res, err := d.db.Exec(query)

	if err != nil {
		return fmt.Errorf("Failed to update request: '%s': %q", sr.IgoRequestID, err)
	}

	var expectedCount int64 = 1
	updatedCount, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("Failed to fetch rows affected request: '%s': %q", sr.IgoRequestID, err)
	}
	if updatedCount != expectedCount {
		return fmt.Errorf("Expected '%d' rows updated but got '%d', request: '%s'", expectedCount, updatedCount, sr.IgoRequestID)
	}

	return nil
}

func (d *DatabricksService) GetRequest(igoRequestID string) (SmileRequest, error) {
	var toReturn SmileRequest
	if err := d.db.Ping(); err != nil {
		errReturn := fmt.Errorf("Failed to connect to database request: '%s': %q", igoRequestID, err)
		return toReturn, errReturn
	}
	query := fmt.Sprintf("select REQUEST_JSON from %s.%s where IGO_REQUEST_ID = '%s'", d.schema, d.requestTable, igoRequestID)
	var rJSON sql.NullString
	err := d.db.QueryRow(query).Scan(&rJSON)
	if err != nil {
		errReturn := fmt.Errorf("Failed to get request: '%s': %q", igoRequestID, err)
		return toReturn, errReturn
	}
	if rJSON.Valid {
		err = json.Unmarshal([]byte(rJSON.String), &toReturn)
		if err != nil {
			errReturn := fmt.Errorf("Failed to Unmarshal request: '%s': %q", igoRequestID, err)
			return toReturn, errReturn
		}
	}
	return toReturn, nil
}

func (d *DatabricksService) RemoveRequest(igoRequestID string) error {
	if err := d.db.Ping(); err != nil {
		return fmt.Errorf("Failed to connect to database request: '%s': %q", igoRequestID, err)
	}
	query := fmt.Sprintf("delete from %s.%s where IGO_REQUEST_ID = '%s'", d.schema, d.requestTable, igoRequestID)
	_, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to remove request: '%s': %q", igoRequestID, err)
	}
	return nil
}

func (d *DatabricksService) InsertSamples(igoRequestID string, samples []SmileSample) error {
	if err := d.db.Ping(); err != nil {
		return fmt.Errorf("Failed to connect to database request: '%s': %q", igoRequestID, err)
	}

	expectedCount := int64(len(samples))
	var insertedCount int64
	for _, s := range samples {
		sJson, err := json.Marshal(s)
		if err != nil {
			return fmt.Errorf("Failed to marshal sample: '%s', request '%s': %q", s.SampleName, igoRequestID, err)
		}
		query := fmt.Sprintf("insert into %s.%s values ('%s', '%s', '%s', '%s', '%s', '%s')", d.schema, d.sampleTable, igoRequestID, s.SampleName, s.CmoSampleName, s.CFDNA2DBarcode, s.CmoPatientID, sJson)
		res, err := d.db.Exec(query)
		if err != nil {
			return fmt.Errorf("Failed to insert sample: '%s', request '%s': %q", s.SampleName, igoRequestID, err)
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return fmt.Errorf("Failed to fetch rows affected sample: '%s', request '%s': %q", s.SampleName, igoRequestID, err)
		}
		insertedCount = insertedCount + rowsAffected
	}
	if insertedCount != expectedCount {
		return fmt.Errorf("Expected '%d' sample rows inserted but got '%d', request: '%s'", expectedCount, insertedCount, igoRequestID)
	}
	return nil
}

func (d *DatabricksService) UpdateSample(igoRequestID string, s SmileSample) error {
	if err := d.db.Ping(); err != nil {
		return fmt.Errorf("Failed to connect to database request: '%s': %q", igoRequestID, err)
	}

	sJson, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("Failed to marshal sample: '%s', request '%s': %q", s.SampleName, igoRequestID, err)
	}

	query := fmt.Sprintf("update %s.%s set CMO_SAMPLE_NAME = '%s', CFDNA2DBARCODE = '%s', CMO_PATIENT_ID = '%s', SAMPLE_JSON = '%s' where IGO_REQUEST_ID = '%s' and IGO_SAMPLE_NAME = '%s'",
		d.schema, d.sampleTable, s.CmoSampleName, s.CFDNA2DBarcode, s.CmoPatientID, string(sJson), igoRequestID, s.SampleName)

	res, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to update sample: '%s', request '%s': %q", s.SampleName, igoRequestID, err)
	}

	var expectedCount int64 = 1
	updatedCount, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("Failed to fetch rows affected sample: '%s', request '%s': %q", s.SampleName, igoRequestID, err)
	}

	if updatedCount != expectedCount {
		return fmt.Errorf("Expected '%d' rows updated but got '%d', sample: '%s', request: '%s'", expectedCount, updatedCount, s.SampleName, igoRequestID)
	}

	return nil
}

func (d *DatabricksService) GetSample(igoRequestID, sampleName string) (SmileSample, error) {
	var toReturn SmileSample
	if err := d.db.Ping(); err != nil {
		errReturn := fmt.Errorf("Failed to connect to database request: '%s': %q", igoRequestID, err)
		return toReturn, errReturn
	}
	query := fmt.Sprintf("select SAMPLE_JSON from %s.%s where IGO_REQUEST_ID = '%s' and IGO_SAMPLE_NAME = '%s'", d.schema, d.sampleTable, igoRequestID, sampleName)
	var sJSON sql.NullString
	err := d.db.QueryRow(query).Scan(&sJSON)
	if err != nil {
		errReturn := fmt.Errorf("Failed to get sample: '%s', request '%s': %q", sampleName, igoRequestID, err)
		return toReturn, errReturn
	}
	if sJSON.Valid {
		err = json.Unmarshal([]byte(sJSON.String), &toReturn)
		if err != nil {
			errReturn := fmt.Errorf("Failed to unmarshal sample: '%s', request '%s': %q", sampleName, igoRequestID, err)
			return toReturn, errReturn
		}
	}
	return toReturn, nil
}

func (d *DatabricksService) RemoveSamples(igoRequestID string) error {
	if err := d.db.Ping(); err != nil {
		return fmt.Errorf("Failed to connect to database request: '%s': %q", igoRequestID, err)
	}
	query := fmt.Sprintf("delete from %s.%s where IGO_REQUEST_ID = '%s'", d.schema, d.sampleTable, igoRequestID)
	_, err := d.db.Exec(query)
	if err != nil {
		return fmt.Errorf("Failed to remove samples for request: '%s': %q", igoRequestID, err)
	}
	return nil
}

func openDatabase(accessToken, hostname, httpPath string, port int) (*sql.DB, error) {
	connector, err := dbsql.NewConnector(
		dbsql.WithAccessToken(accessToken),
		dbsql.WithServerHostname(hostname),
		dbsql.WithPort(port),
		dbsql.WithHTTPPath(httpPath),
	)

	if err != nil {
		return nil, err
	}

	return sql.OpenDB(connector), nil
}
