package smile_databricks_gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"

	nm "github.com/mskcc/nats-messaging-go"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type SmileService struct {
	databricksService *DatabricksService
	natsMessaging     *nm.Messaging
}

type RequestAdapter struct {
	Requests []SmileRequest
	Msg      *nm.Msg
}

type SampleAdapter struct {
	Samples []SmileSample
	Msg     *nm.Msg
}

const (
	requestBufSize = 1
	sampleBufSize  = 1
)

func NewSmileService(url, certPath, keyPath, consumer, password string, databricksService *DatabricksService) (*SmileService, error) {
	natsMessaging, err := nm.NewSecureMessaging(url, certPath, keyPath, consumer, password)
	if err != nil {
		return nil, fmt.Errorf("Failed to create a nats messaging client: %q", err)
	}
	return &SmileService{databricksService: databricksService, natsMessaging: natsMessaging}, nil
}

const (
	runMsg                 = "Entering smile_service.Run() loop iteration"
	runSubscribeMsg        = "Subscribing to SMILE message topics"
	runLoopSpanMsg         = "Entering select on incoming SMILE messages"
	newReqInsertMsg        = "Attempting to insert new request in databricks"
	IGORequestIdKey        = "IGO Request ID"
	newReqDBInsertErrMsg   = "Error inserting new request into Databricks"
	newReqDBInsertSucMsg   = "Successfully inserted new request into Databricks"
	newSampDBInsertErrMsg  = "Error inserting new samples into Databricks"
	newSampDBInsertSucMsg  = "Successfully inserted new samples into Databricks"
	updateRequestMsg       = "Attempting to update a request in databricks"
	upReqDBUpdateErrMsg    = "Error updating request in Databricks"
	upReqDBUpdateSucMsg    = "Successfully updated request into Databricks"
	SampleNameKey          = "Sample Name"
	updateSampleMsg        = "Attempting to update a sample in databricks"
	upSampleDBUpdateErrMsg = "Error updating sample in Databricks"
	upSampleDBUpdateSucMsg = "Successfully updated sample into Databricks"
)

func (ss *SmileService) Run(ctx context.Context, consumer, subject, newRequestFilter, updateRequestFilter, updateSampleFilter string, tracer trace.Tracer) error {

	runCtx, runSpan := tracer.Start(ctx, runMsg)

	newRequestChan := make(chan RequestAdapter, requestBufSize)
	updateRequestChan := make(chan RequestAdapter, requestBufSize)
	updateSampleChan := make(chan SampleAdapter, sampleBufSize)
	runSpan.AddEvent(runSubscribeMsg)
	err := ss.subscribeToService(runCtx, newRequestChan, updateRequestChan, updateSampleChan,
		consumer, subject, newRequestFilter, updateRequestFilter, updateSampleFilter, tracer)
	if err != nil {
		return err
	}

	var nrwg sync.WaitGroup
	var urwg sync.WaitGroup
	var uswg sync.WaitGroup
	for {

		selectCtx, runLoopSpan := tracer.Start(runCtx, runLoopSpanMsg)

		select {
		case ra := <-newRequestChan:
			_, nrSpan := tracer.Start(selectCtx, newReqInsertMsg)
			nrSpan.SetAttributes(attribute.String(IGORequestIdKey, ra.Requests[0].IgoRequestID))
			nrwg.Add(1)
			go func() {
				defer nrwg.Done()
				err := ss.databricksService.InsertRequest(ra.Requests[0])
				if handleError(err, newReqDBInsertErrMsg, nrSpan) {
					return
				}
				nrSpan.AddEvent(newReqDBInsertSucMsg)
				err = ss.databricksService.InsertSamples(ra.Requests[0].IgoRequestID, ra.Requests[0].Samples)
				if handleError(err, newSampDBInsertErrMsg, nrSpan) {
					return
				}
				nrSpan.AddEvent(newSampDBInsertSucMsg)
				ra.Msg.ProviderMsg.Ack()
			}()
		case ra := <-updateRequestChan:
			_, urSpan := tracer.Start(selectCtx, updateRequestMsg)
			urSpan.SetAttributes(attribute.String(IGORequestIdKey, ra.Requests[0].IgoRequestID))
			urwg.Add(1)
			go func() {
				defer urwg.Done()
				err := ss.databricksService.UpdateRequest(ra.Requests[0])
				if handleError(err, upReqDBUpdateErrMsg, urSpan) {
					return
				}
				urSpan.AddEvent(upReqDBUpdateSucMsg)
				ra.Msg.ProviderMsg.Ack()
			}()
		case sa := <-updateSampleChan:
			_, usSpan := tracer.Start(selectCtx, updateSampleMsg)
			usSpan.SetAttributes(attribute.String(IGORequestIdKey, sa.Samples[0].AdditionalProperties.IgoRequestID))
			usSpan.SetAttributes(attribute.String(SampleNameKey, sa.Samples[0].SampleName))
			uswg.Add(1)
			go func() {
				defer uswg.Done()
				err := ss.databricksService.UpdateSample(sa.Samples[0].AdditionalProperties.IgoRequestID, sa.Samples[0])
				if handleError(err, upSampleDBUpdateErrMsg, usSpan) {
					return
				}
				usSpan.AddEvent(upSampleDBUpdateSucMsg)
				sa.Msg.ProviderMsg.Ack()
			}()
		case <-ctx.Done():
			log.Println("Context canceled, returning...")
			nrwg.Wait()
			urwg.Wait()
			uswg.Wait()
			ss.natsMessaging.Shutdown()
			runLoopSpan.End()
			return nil
		}
		runLoopSpan.End()
	}
}

const (
	incomingNewReqMsg      = "New incoming request"
	processingNewReqErrMsg = "Error unmarshaling new incoming request"
	processingNewReqSucMsg = "Successfully unmarshaled new incoming request"

	incomingUpReqMsg      = "Updated incoming request"
	processingUpReqErrMsg = "Error unmarshaling updated incoming request"
	processingUpReqSucMsg = "Successfully unmarshaled updated incoming request"

	incomingUpSampMsg      = "Updated incoming sample"
	processingUpSampErrMsg = "Error unmarshaling updated incoming sample"
	processingUpSampSucMsg = "Successfully unmarshaled updated incoming sample"
)

func (ss *SmileService) subscribeToService(ctx context.Context, newRequestCh, upRequestCh chan RequestAdapter, upSampleCh chan SampleAdapter,
	consumer, subject, newRequestFilter, updateRequestFilter, updateSampleFilter string, tracer trace.Tracer) error {
	err := ss.natsMessaging.Subscribe(consumer, subject, func(m *nm.Msg) {
		switch {
		case m.Subject == newRequestFilter:
			_, nrSpan := tracer.Start(ctx, incomingNewReqMsg)
			nr, err := unMarshal[SmileRequest](string(m.Data))
			if handleError(err, processingNewReqErrMsg, nrSpan) {
				break
			}
			nrSpan.AddEvent(processingNewReqSucMsg)
			nrSpan.End()
			newRequestCh <- RequestAdapter{[]SmileRequest{nr}, m}
		case m.Subject == updateRequestFilter:
			_, urSpan := tracer.Start(ctx, incomingUpReqMsg)
			ru, err := unMarshal[[]SmileRequest](string(m.Data))
			if handleError(err, processingUpReqErrMsg, urSpan) {
				break
			}
			urSpan.AddEvent(processingUpReqSucMsg)
			urSpan.End()
			upRequestCh <- RequestAdapter{ru, m}
		case m.Subject == updateSampleFilter:
			_, usSpan := tracer.Start(ctx, incomingUpSampMsg)
			su, err := unMarshal[[]SmileSample](string(m.Data))
			if handleError(err, processingUpSampErrMsg, usSpan) {
				break
			}
			usSpan.AddEvent(processingUpSampSucMsg)
			usSpan.End()
			upSampleCh <- SampleAdapter{su, m}
		default:
			// not interested in message, Ack it so we don't get it again
			m.ProviderMsg.Ack()
		}
	})
	return err
}

func unMarshal[T any](msgData string) (T, error) {
	var target T
	unquoted, err := strconv.Unquote(msgData)
	if err != nil {
		return target, err
	}
	if err := json.Unmarshal([]byte(unquoted), &target); err != nil {
		return target, err
	}
	return target, nil
}

func handleError(err error, message string, span trace.Span) bool {
	if err != nil {
		msg := fmt.Sprintf("%s: %v", message, err)
		span.AddEvent(msg)
		span.SetStatus(codes.Error, msg)
		span.End()
		return true
	}
	return false
}
