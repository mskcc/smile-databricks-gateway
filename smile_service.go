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
	awsS3Service      *AWSS3Service
	databricksService *DatabricksService
	natsMessaging     *nm.Messaging
}

type RequestAdapter struct {
	Requests []SmileRequest
	Msg      *nm.Msg
	SpanCtx  context.Context
}

type SampleAdapter struct {
	Samples []SmileSample
	Msg     *nm.Msg
	SpanCtx context.Context
}

const (
	requestBufSize = 1
	sampleBufSize  = 1
)

func NewSmileService(url, certPath, keyPath, consumer, password string, awsS3Service *AWSS3Service, databricksService *DatabricksService) (*SmileService, error) {
	natsMessaging, err := nm.NewSecureMessaging(url, certPath, keyPath, consumer, password)
	if err != nil {
		return nil, fmt.Errorf("Failed to create a nats messaging client: %q", err)
	}
	return &SmileService{awsS3Service: awsS3Service, databricksService: databricksService, natsMessaging: natsMessaging}, nil
}

const (
	newReqS3WriteMsg       = "Attempting to write new request into S3 bucket"
	IGORequestIdKey        = "IGO Request ID"
	newReqS3WriteErrMsg    = "Error writing new request into S3 bucket"
	newReqS3WriteSucMsg    = "Successfully wrote new request into S3 bucket"
	succProcessedNewReqMsg = "Successfully processed new request: %d"
	newSampleS3WriteErrMsg = "Error writing new sample into S3 bucket"
	newSampleS3WriteSucMsg = "Successfully wrote new sample into S3 bucket"
	NumSamplesWrittenKey   = "Num Samples Written"
	updateReqS3WriteMsg    = "Attempting to update a request in an S3 bucket"
	upReqS3WriteErrMsg     = "Error updating request in an S3 bucket"
	upReqS3WriteSucMsg     = "Successfully updated request in an S3 bucket"
	succProcessedUpReqMsg  = "Successfully processed request update: %d"
	SampleNameKey          = "Sample Name"
	updateSampleS3WriteMsg = "Attempting to update a sample in an S3 bucket"
	upSampleS3WriteErrMsg  = "Error updating sample in an S3 bucket"
	upSampleS3WriteSucMsg  = "Successfully updated a sample in an S3 bucket"
	succProcessedUpSampMsg = "Successfully processed sample update: %d"
	execDLTPipelineErrMsg  = "Unsuccessfully executed DLT pipeline"
	execDLTPipelineSucMsg  = "Successfully executed DLT pipeline"
	errSlackNotifMsg       = "Error sending slack notification"
	succSlackNotifMsg      = "Successfully sent slack notification"
)

func (ss *SmileService) Run(ctx context.Context, consumer, subject, newRequestFilter, updateRequestFilter, updateSampleFilter string, tracer trace.Tracer, slackURL string) error {

	newRequestChan := make(chan RequestAdapter, requestBufSize)
	updateRequestChan := make(chan RequestAdapter, requestBufSize)
	updateSampleChan := make(chan SampleAdapter, sampleBufSize)
	err := ss.subscribeToService(ctx, newRequestChan, updateRequestChan, updateSampleChan,
		consumer, subject, newRequestFilter, updateRequestFilter, updateSampleFilter, tracer)
	if err != nil {
		return err
	}

	var nrwg sync.WaitGroup
	var urwg sync.WaitGroup
	var uswg sync.WaitGroup
	for {
		select {
		case ra := <-newRequestChan:
			nrCtx, nrSpan := tracer.Start(ra.SpanCtx, newReqS3WriteMsg)
			nrSpan.SetAttributes(attribute.String(IGORequestIdKey, ra.Requests[0].IgoRequestID))
			nrwg.Add(1)
			go func() {
				defer nrwg.Done()
				filename := fmt.Sprintf("%s_request.json", ra.Requests[0].IgoRequestID)
				// pull samples out of request and persist them separately
				samples := ra.Requests[0].Samples
				ra.Requests[0].Samples = nil
				err := ss.awsS3Service.PutRequest(filename, ra.Requests[0])
				if handleError(err, newReqS3WriteErrMsg, nrSpan) {
					return
				}
				for _, sample := range samples {
					filename := fmt.Sprintf("%s_sample.json", sample.SampleName)
					err := ss.awsS3Service.PutSample(filename, sample)
					if handleError(err, newSampleS3WriteErrMsg, nrSpan) {
						return
					}
					nrSpan.AddEvent(newSampleS3WriteSucMsg, trace.WithAttributes(attribute.String(SampleNameKey, sample.SampleName)))
				}
				nrSpan.SetAttributes(attribute.Int(NumSamplesWrittenKey, len(samples)))
				nrSpan.AddEvent(newReqS3WriteSucMsg, trace.WithAttributes(attribute.String(IGORequestIdKey, ra.Requests[0].IgoRequestID)))
				ra.Msg.ProviderMsg.Ack()
				err = ss.databricksService.ExecutePipeline(nrCtx)
				if handleError(err, execDLTPipelineErrMsg, nrSpan) {
					return
				}
				nrSpan.AddEvent(execDLTPipelineSucMsg)
				mesg := fmt.Sprintf("{\"text\":\"New request written to Databricks:\n\tRequest Id: %s\"}", ra.Requests[0].IgoRequestID)
				err = NotifyViaSlack(nrCtx, mesg, slackURL)
				if handleError(err, errSlackNotifMsg, nrSpan) {
					return
				}
				nrSpan.AddEvent(succSlackNotifMsg)
				nrSpan.SetStatus(codes.Ok, fmt.Sprintf(succProcessedNewReqMsg, ra.Requests[0].IgoRequestID))
				nrSpan.End()
			}()
		case ra := <-updateRequestChan:
			urCtx, urSpan := tracer.Start(ra.SpanCtx, updateReqS3WriteMsg)
			urSpan.SetAttributes(attribute.String(IGORequestIdKey, ra.Requests[0].IgoRequestID))
			urwg.Add(1)
			go func() {
				defer urwg.Done()
				filename := fmt.Sprintf("%s_request.json", ra.Requests[0].IgoRequestID)
				err := ss.awsS3Service.PutRequest(filename, ra.Requests[0])
				if handleError(err, upReqS3WriteErrMsg, urSpan) {
					return
				}
				urSpan.AddEvent(upReqS3WriteSucMsg)
				ra.Msg.ProviderMsg.Ack()
				err = ss.databricksService.ExecutePipeline(urCtx)
				if handleError(err, execDLTPipelineErrMsg, urSpan) {
					return
				}
				urSpan.AddEvent(execDLTPipelineSucMsg)
				mesg := fmt.Sprintf("{\"text\":\"Updated request written to Databricks:\n\tRequest Id: %s\"}", ra.Requests[0].IgoRequestID)
				err = NotifyViaSlack(urCtx, mesg, slackURL)
				if handleError(err, errSlackNotifMsg, urSpan) {
					return
				}
				urSpan.AddEvent(succSlackNotifMsg)
				urSpan.SetStatus(codes.Ok, fmt.Sprintf(succProcessedUpReqMsg, ra.Requests[0].IgoRequestID))
				urSpan.End()
			}()
		case sa := <-updateSampleChan:
			usCtx, usSpan := tracer.Start(sa.SpanCtx, updateSampleS3WriteMsg)
			usSpan.SetAttributes(attribute.String(IGORequestIdKey, sa.Samples[0].AdditionalProperties.IgoRequestID))
			usSpan.SetAttributes(attribute.String(SampleNameKey, sa.Samples[0].SampleName))
			uswg.Add(1)
			go func() {
				defer uswg.Done()
				filename := fmt.Sprintf("%s_sample.json", sa.Samples[0].SampleName)
				err := ss.awsS3Service.PutSample(filename, sa.Samples[0])
				if handleError(err, upSampleS3WriteErrMsg, usSpan) {
					return
				}
				usSpan.AddEvent(upSampleS3WriteSucMsg)
				sa.Msg.ProviderMsg.Ack()
				err = ss.databricksService.ExecutePipeline(usCtx)
				if handleError(err, execDLTPipelineErrMsg, usSpan) {
					return
				}
				usSpan.AddEvent(execDLTPipelineSucMsg)
				mesg := fmt.Sprintf("{\"text\":\"Updated sample written to Databricks:\n\tSample Name: %s\"}", sa.Samples[0].SampleName)
				err = NotifyViaSlack(usCtx, mesg, slackURL)
				if handleError(err, errSlackNotifMsg, usSpan) {
					return
				}
				usSpan.AddEvent(succSlackNotifMsg)
				usSpan.SetStatus(codes.Ok, fmt.Sprintf(succProcessedUpSampMsg, sa.Samples[0].SampleName))
				usSpan.End()
			}()
		case <-ctx.Done():
			log.Println("Context canceled, returning...")
			nrwg.Wait()
			urwg.Wait()
			uswg.Wait()
			ss.natsMessaging.Shutdown()
			return nil
		}
	}
}

const (
	incomingNewReqMsg      = "New incoming request"
	processingNewReqErrMsg = "Error unmarshaling new incoming request"
	processingNewReqSucMsg = "Successfully unmarshaled new incoming request"

	incomingUpReqMsg      = "Updated incoming request"
	processingUpReqErrMsg = "Error unmarshaling updated incoming request"
	processingUpReqSucMsg = "Successfully received and unmarshaled updated incoming request"

	incomingUpSampMsg      = "Updated incoming sample"
	processingUpSampErrMsg = "Error unmarshaling updated incoming sample"
	processingUpSampSucMsg = "Successfully received and unmarshaled updated incoming sample"

	handingOffRequestToRunLoopMsg = "Handing off request for writing to S3 bucket connected to Databricks"
	handingOffSampleToRunLoopMsg  = "Handing off sample for writing to an S3 bucket connected to Databricks"
)

func (ss *SmileService) subscribeToService(ctx context.Context, newRequestCh, upRequestCh chan RequestAdapter, upSampleCh chan SampleAdapter,
	consumer, subject, newRequestFilter, updateRequestFilter, updateSampleFilter string, tracer trace.Tracer) error {
	err := ss.natsMessaging.Subscribe(consumer, subject, func(m *nm.Msg) {
		switch {
		case m.Subject == newRequestFilter:
			subscribeCtx, nrSpan := tracer.Start(ctx, incomingNewReqMsg)
			nr, err := unMarshal[SmileRequest](string(m.Data))
			if handleError(err, processingNewReqErrMsg, nrSpan) {
				break
			}
			nrSpan.AddEvent(processingNewReqSucMsg, trace.WithAttributes(attribute.String(IGORequestIdKey, nr.IgoRequestID)))
			nrSpan.AddEvent(handingOffRequestToRunLoopMsg, trace.WithAttributes(attribute.String(IGORequestIdKey, nr.IgoRequestID)))
			nrSpan.End()
			newRequestCh <- RequestAdapter{[]SmileRequest{nr}, m, subscribeCtx}
		case m.Subject == updateRequestFilter:
			subscribeCtx, urSpan := tracer.Start(ctx, incomingUpReqMsg)
			ru, err := unMarshal[[]SmileRequest](string(m.Data))
			if handleError(err, processingUpReqErrMsg, urSpan) {
				break
			}
			urSpan.AddEvent(processingUpReqSucMsg, trace.WithAttributes(attribute.String(IGORequestIdKey, ru[0].IgoRequestID)))
			urSpan.AddEvent(handingOffRequestToRunLoopMsg, trace.WithAttributes(attribute.String(IGORequestIdKey, ru[0].IgoRequestID)))
			urSpan.End()
			upRequestCh <- RequestAdapter{ru, m, subscribeCtx}
		case m.Subject == updateSampleFilter:
			subscribeCtx, usSpan := tracer.Start(ctx, incomingUpSampMsg)
			su, err := unMarshal[[]SmileSample](string(m.Data))
			if handleError(err, processingUpSampErrMsg, usSpan) {
				break
			}
			usSpan.AddEvent(processingUpSampSucMsg, trace.WithAttributes(attribute.String(SampleNameKey, su[0].SampleName)))
			usSpan.AddEvent(handingOffSampleToRunLoopMsg, trace.WithAttributes(attribute.String(SampleNameKey, su[0].SampleName)))
			usSpan.End()
			upSampleCh <- SampleAdapter{su, m, subscribeCtx}
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
