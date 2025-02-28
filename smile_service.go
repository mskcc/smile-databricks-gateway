package smile_databricks_gateway

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"

	nm "github.com/mskcc/nats-messaging-go"
	st "github.mskcc.org/cdsi/cdsi-protobuf/smile/generated/v1/go"
	"google.golang.org/protobuf/proto"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type SmileService struct {
	awsS3Service  *AWSS3Service
	natsMessaging *nm.Messaging
}

type IGORequestAdapter struct {
	Requests []SmileRequest
	Msg      *nm.Msg
	SpanCtx  context.Context
}

type IGOSampleAdapter struct {
	Samples []SmileSample
	Msg     *nm.Msg
	SpanCtx context.Context
}

type TEMPOSampleAdapter struct {
	Samples []*st.TempoSample
	Msg     *nm.Msg
	SpanCtx context.Context
}

const (
	igoRequestBufSize  = 1
	igoSampleBufSize   = 1
	tempoSampleBufSize = 1
)

func NewSmileService(url, certPath, keyPath, consumer, password string, awsS3Service *AWSS3Service) (*SmileService, error) {
	natsMessaging, err := nm.NewSecureMessaging(url, certPath, keyPath, consumer, password)
	if err != nil {
		return nil, fmt.Errorf("Failed to create a nats messaging client: %q", err)
	}
	return &SmileService{awsS3Service: awsS3Service, natsMessaging: natsMessaging}, nil
}

const (
	newIGOReqS3WriteMsg       = "Attempting to write new IGO request into S3 bucket"
	IGORequestIdKey           = "IGO Request ID"
	newIGOReqS3WriteErrMsg    = "Error writing new IGO request into S3 bucket"
	newIGOReqS3WriteSucMsg    = "Successfully wrote new IGO request into S3 bucket"
	succProcessNewIGOReqMsg   = "Successfully processed new IGO request: %s"
	newIGOSampleS3WriteErrMsg = "Error writing new IGO sample into S3 bucket"
	newIGOSampleS3WriteSucMsg = "Successfully wrote new IGO sample into S3 bucket"
	NumSamplesWrittenKey      = "Num Samples Written"
	updateIGOReqS3WriteMsg    = "Attempting to update an IGO request in an S3 bucket"
	upIGOReqS3WriteErrMsg     = "Error updating IGO request in an S3 bucket"
	upIGOReqS3WriteSucMsg     = "Successfully updated IGO request in an S3 bucket"
	succProcessedUpIGOReqMsg  = "Successfully processed IGO request update: %s"
	IGOSampleNameKey          = "IGO Sample Name"
	updateIGOSampleS3WriteMsg = "Attempting to update an IGO sample in an S3 bucket"
	upIGOSampleS3WriteErrMsg  = "Error updating IGO sample in an S3 bucket"
	upIGOSampleS3WriteSucMsg  = "Successfully updated an IGO sample in an S3 bucket"
	succProcessedUpIGOSampMsg = "Successfully processed an IGO sample update: %s"

	TEMPOReleasedWriteMsg             = "Attempting to write released TEMPO Samples into S3 bucket"
	TEMPOReleasedSamplesS3WriteErrMsg = "Error writing released TEMPO sample into S3 bucket"
	TEMPOReleasedSamplesS3WriteSucMsg = "Successfully wrote released TEMPO sample into S3 bucket"
	succProcessTEMPOReleasedMsg       = "Successfully processed released TEMPO Samples"
	TEMPOUpdatedWriteMsg              = "Attempting to write updated TEMPO Samples into S3 bucket"
	TEMPOUpdatedSamplesS3WriteErrMsg  = "Error writing updated TEMPO sample into S3 bucket"
	TEMPOUpdatedSamplesS3WriteSucMsg  = "Successfully wrote updated TEMPO sample into S3 bucket"
	succProcessTEMPOUpdatedMsg        = "Successfully processed updated TEMPO Samples"
	TEMPOSampleNamesKey               = "TEMPO Sample Names"
	TEMPOSampleNameKey                = "TEMPO Sample Name"

	errSlackNotifMsg  = "Error sending slack notification"
	succSlackNotifMsg = "Successfully sent slack notification"
)

func (ss *SmileService) Run(ctx context.Context, consumer, subjectFilter, newIGORequestFilter, updateIGORequestFilter, updateIGOSampleFilter, igoAWSBucket, releaseTEMPOSamplesFilter, updateTEMPOSampleFilter, tempoAWSBucket string, tracer trace.Tracer, slackURL string) error {
	newIGORequestChan := make(chan IGORequestAdapter, igoRequestBufSize)
	updateIGORequestChan := make(chan IGORequestAdapter, igoRequestBufSize)
	updateIGOSampleChan := make(chan IGOSampleAdapter, igoSampleBufSize)
	releaseTEMPOSamplesChan := make(chan TEMPOSampleAdapter, tempoSampleBufSize)
	updateTEMPOSamplesChan := make(chan TEMPOSampleAdapter, tempoSampleBufSize)
	// a nats consumer can only have one subject filter when created, so we need to have a single event handler
	err := ss.subscribeToSubjects(ctx, consumer, subjectFilter, newIGORequestChan, updateIGORequestChan, updateIGOSampleChan, newIGORequestFilter, updateIGORequestFilter, updateIGOSampleFilter,
		releaseTEMPOSamplesChan, updateTEMPOSamplesChan, releaseTEMPOSamplesFilter, updateTEMPOSampleFilter, tracer)
	if err != nil {
		return err
	}

	var nigorwg sync.WaitGroup
	var uigorwg sync.WaitGroup
	var uigoswg sync.WaitGroup
	var trswg sync.WaitGroup
	var tuswg sync.WaitGroup
	for {
		select {
		case ra := <-newIGORequestChan:
			nrCtx, nrSpan := tracer.Start(ra.SpanCtx, newIGOReqS3WriteMsg)
			nrSpan.SetAttributes(attribute.String(IGORequestIdKey, ra.Requests[0].IgoRequestID))
			nigorwg.Add(1)
			go ss.processNewIGORequest(nrCtx, nigorwg, nrSpan, ra, igoAWSBucket, slackURL)
		case ra := <-updateIGORequestChan:
			urCtx, urSpan := tracer.Start(ra.SpanCtx, updateIGOReqS3WriteMsg)
			urSpan.SetAttributes(attribute.String(IGORequestIdKey, ra.Requests[0].IgoRequestID))
			uigorwg.Add(1)
			go ss.processUpdateIGORequest(urCtx, uigorwg, urSpan, ra, igoAWSBucket, slackURL)
		case sa := <-updateIGOSampleChan:
			usCtx, usSpan := tracer.Start(sa.SpanCtx, updateIGOSampleS3WriteMsg)
			usSpan.SetAttributes(attribute.String(IGORequestIdKey, sa.Samples[0].AdditionalProperties.IgoRequestID))
			usSpan.SetAttributes(attribute.String(IGOSampleNameKey, sa.Samples[0].SampleName))
			uigoswg.Add(1)
			go ss.processUpdateIGOSample(usCtx, uigoswg, usSpan, sa, igoAWSBucket, slackURL)
		case tsa := <-releaseTEMPOSamplesChan:
			tsaCtx, tsaSpan := tracer.Start(tsa.SpanCtx, TEMPOReleasedWriteMsg)
			trswg.Add(1)
			go ss.processTEMPOSamples(tsaCtx, trswg, tsaSpan, tsa, TEMPOReleasedSamplesS3WriteErrMsg, TEMPOReleasedSamplesS3WriteSucMsg, succProcessTEMPOReleasedMsg, tempoAWSBucket, slackURL)
		case tsa := <-updateTEMPOSamplesChan:
			tsaCtx, tsaSpan := tracer.Start(tsa.SpanCtx, TEMPOUpdatedWriteMsg)
			tuswg.Add(1)
			go ss.processTEMPOSamples(tsaCtx, tuswg, tsaSpan, tsa, TEMPOUpdatedSamplesS3WriteErrMsg, TEMPOUpdatedSamplesS3WriteSucMsg, succProcessTEMPOUpdatedMsg, tempoAWSBucket, slackURL)
		case <-ctx.Done():
			log.Println("Context canceled, returning...")
			nigorwg.Wait()
			uigorwg.Wait()
			uigoswg.Wait()
			trswg.Wait()
			tuswg.Wait()
			ss.natsMessaging.Shutdown()
			return nil
		}
	}
}

func (ss *SmileService) processNewIGORequest(nrCtx context.Context, nigorwg sync.WaitGroup, nrSpan trace.Span, ra IGORequestAdapter, igoAWSBucket, slackURL string) {
	defer nigorwg.Done()
	filename := fmt.Sprintf("%s_request.json", ra.Requests[0].IgoRequestID)
	// pull samples out of request and persist them separately
	samples := ra.Requests[0].Samples
	ra.Requests[0].Samples = nil
	err := ss.awsS3Service.PutRequest(filename, igoAWSBucket, ra.Requests[0])
	if handleError(err, newIGOReqS3WriteErrMsg, nrSpan) {
		return
	}
	for _, sample := range samples {
		filename := fmt.Sprintf("%s_sample.json", sample.PrimaryID)
		err := ss.awsS3Service.PutIGOSample(filename, igoAWSBucket, sample)
		if handleError(err, newIGOSampleS3WriteErrMsg, nrSpan) {
			return
		}
		nrSpan.AddEvent(newIGOSampleS3WriteSucMsg, trace.WithAttributes(attribute.String(IGOSampleNameKey, sample.PrimaryID)))
	}
	nrSpan.SetAttributes(attribute.Int(NumSamplesWrittenKey, len(samples)))
	nrSpan.AddEvent(newIGOReqS3WriteSucMsg, trace.WithAttributes(attribute.String(IGORequestIdKey, ra.Requests[0].IgoRequestID), attribute.Int(NumSamplesWrittenKey, len(samples))))
	ra.Msg.ProviderMsg.Ack()
	mesg := fmt.Sprintf("{\"text\":\"New IGO request written to Databricks S3 bucket:\n\tRequest Id: %s\"}", ra.Requests[0].IgoRequestID)
	err = NotifyViaSlack(nrCtx, mesg, slackURL)
	if handleError(err, errSlackNotifMsg, nrSpan) {
		return
	}
	nrSpan.AddEvent(succSlackNotifMsg)
	nrSpan.SetStatus(codes.Ok, fmt.Sprintf(succProcessNewIGOReqMsg, ra.Requests[0].IgoRequestID))
	nrSpan.End()
}

func (ss *SmileService) processUpdateIGORequest(urCtx context.Context, uigorwg sync.WaitGroup, urSpan trace.Span, ra IGORequestAdapter, igoAWSBucket, slackURL string) {
	defer uigorwg.Done()
	// last request is most recently updated
	indLast := len(ra.Requests) - 1
	filename := fmt.Sprintf("%s_request.json", ra.Requests[indLast].IgoRequestID)
	err := ss.awsS3Service.PutRequest(filename, igoAWSBucket, ra.Requests[indLast])
	if handleError(err, upIGOReqS3WriteErrMsg, urSpan) {
		return
	}
	urSpan.AddEvent(upIGOReqS3WriteSucMsg)
	ra.Msg.ProviderMsg.Ack()
	mesg := fmt.Sprintf("{\"text\":\"Updated IGO request written to Databricks S3 bucket:\n\tRequest Id: %s\"}", ra.Requests[indLast].IgoRequestID)
	err = NotifyViaSlack(urCtx, mesg, slackURL)
	if handleError(err, errSlackNotifMsg, urSpan) {
		return
	}
	urSpan.AddEvent(succSlackNotifMsg)
	urSpan.SetStatus(codes.Ok, fmt.Sprintf(succProcessedUpIGOReqMsg, ra.Requests[indLast].IgoRequestID))
	urSpan.End()
}

func (ss *SmileService) processUpdateIGOSample(usCtx context.Context, uigoswg sync.WaitGroup, usSpan trace.Span, sa IGOSampleAdapter, igoAWSBucket, slackURL string) {
	defer uigoswg.Done()
	// last sample is most recently updated
	indLast := len(sa.Samples) - 1
	filename := fmt.Sprintf("%s_sample.json", sa.Samples[indLast].PrimaryID)
	err := ss.awsS3Service.PutIGOSample(filename, igoAWSBucket, sa.Samples[indLast])
	if handleError(err, upIGOSampleS3WriteErrMsg, usSpan) {
		return
	}
	usSpan.AddEvent(upIGOSampleS3WriteSucMsg)
	sa.Msg.ProviderMsg.Ack()
	mesg := fmt.Sprintf("{\"text\":\"Updated IGO sample written to Databricks S3 bucket:\n\tSample Name: %s\"}", sa.Samples[indLast].PrimaryID)
	err = NotifyViaSlack(usCtx, mesg, slackURL)
	if handleError(err, errSlackNotifMsg, usSpan) {
		return
	}
	usSpan.AddEvent(succSlackNotifMsg)
	usSpan.SetStatus(codes.Ok, fmt.Sprintf(succProcessedUpIGOSampMsg, sa.Samples[indLast].PrimaryID))
	usSpan.End()
}

func (ss *SmileService) processTEMPOSamples(tsaCtx context.Context, tsawg sync.WaitGroup, tsaSpan trace.Span, tsa TEMPOSampleAdapter, samplePutErrMsg, samplePutSucMsg, sucProcessMsg, tempoAWSBucket, slackURL string) {
	defer tsawg.Done()
	for _, sample := range tsa.Samples {
		filename := fmt.Sprintf("%s_clinical.json", sample.PrimaryId)
		err := ss.awsS3Service.PutTEMPOSample(filename, tempoAWSBucket, *sample)
		if handleError(err, samplePutErrMsg, tsaSpan) {
			return
		}
		tsaSpan.AddEvent(samplePutSucMsg, trace.WithAttributes(attribute.String(TEMPOSampleNameKey, sample.PrimaryId)))
	}
	tsaSpan.SetAttributes(attribute.Int(NumSamplesWrittenKey, len(tsa.Samples)))
	tsa.Msg.ProviderMsg.Ack()
	mesg := fmt.Sprintf("{\"text\":\"TEMPO samples written to Databricks S3 bucket:\n\t%s: %s\"}", TEMPOSampleNamesKey, tsa.Samples)
	err := NotifyViaSlack(tsaCtx, mesg, slackURL)
	if handleError(err, errSlackNotifMsg, tsaSpan) {
		return
	}
	tsaSpan.AddEvent(succSlackNotifMsg)
	tsaSpan.SetStatus(codes.Ok, fmt.Sprintf(sucProcessMsg))
	tsaSpan.End()
}

const (
	incomingNewReqMsg      = "Received new request"
	processingNewReqErrMsg = "Error unmarshaling new request"
	processingNewReqSucMsg = "Successfully unmarshaled new request"

	incomingUpReqMsg      = "Received request update"
	processingUpReqErrMsg = "Error unmarshaling request update"
	processingUpReqSucMsg = "Successfully unmarshaled request update"

	incomingUpSampMsg      = "Received sample update"
	processingUpSampErrMsg = "Error unmarshaling sample update"
	processingUpSampSucMsg = "Successfully unmarshaled sample update"

	handingOffRequestToRunLoopMsg = "Handing off request for writing to S3 bucket connected to Databricks"
	handingOffSampleToRunLoopMsg  = "Handing off sample for writing to an S3 bucket connected to Databricks"

	incomingReleaseTEMPOSamplesMsg      = "Received release TEMPO samples message"
	processingReleaseTEMPOSamplesErrMsg = "Error unmarshaling release TEMPO samples message"
	processingReleaseTEMPOSamplesSucMsg = "Successfully unmarshaled release TEMPO samples message"

	incomingUpTEMPOSamplesMsg      = "Received TEMPO sample update"
	processingUpTEMPOSamplesErrMsg = "Error unmarshaling TEMPO sample updates"
	processingUpTEMPOSamplesSucMsg = "Successfully unmarshaled TEMPO sample updates"

	handingOffTEMPOSamplesToRunLoopMsg = "Handing off TEMPO samples for writing to S3 bucket connected to Databricks"
)

func (ss *SmileService) subscribeToSubjects(ctx context.Context, consumer, subjectFilter string, newRequestCh, upRequestCh chan IGORequestAdapter, upSampleCh chan IGOSampleAdapter, newRequestFilter, updateRequestFilter, updateSampleFilter string,
	releaseTEMPOSamplesCh, updateTEMPOSampleCh chan TEMPOSampleAdapter, releaseTEMPOSamplesFilter, updateTEMPOSampleFilter string, tracer trace.Tracer) error {
	err := ss.natsMessaging.Subscribe(consumer, subjectFilter, func(m *nm.Msg) {
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
			newRequestCh <- IGORequestAdapter{[]SmileRequest{nr}, m, subscribeCtx}
		case m.Subject == updateRequestFilter:
			subscribeCtx, urSpan := tracer.Start(ctx, incomingUpReqMsg)
			ru, err := unMarshal[[]SmileRequest](string(m.Data))
			if handleError(err, processingUpReqErrMsg, urSpan) {
				break
			}
			urSpan.AddEvent(processingUpReqSucMsg, trace.WithAttributes(attribute.String(IGORequestIdKey, ru[0].IgoRequestID)))
			urSpan.AddEvent(handingOffRequestToRunLoopMsg, trace.WithAttributes(attribute.String(IGORequestIdKey, ru[0].IgoRequestID)))
			urSpan.End()
			upRequestCh <- IGORequestAdapter{ru, m, subscribeCtx}
		case m.Subject == updateSampleFilter:
			subscribeCtx, usSpan := tracer.Start(ctx, incomingUpSampMsg)
			su, err := unMarshal[[]SmileSample](string(m.Data))
			if handleError(err, processingUpSampErrMsg, usSpan) {
				break
			}
			usSpan.AddEvent(processingUpSampSucMsg, trace.WithAttributes(attribute.String(IGOSampleNameKey, su[0].SampleName)))
			usSpan.AddEvent(handingOffSampleToRunLoopMsg, trace.WithAttributes(attribute.String(IGOSampleNameKey, su[0].SampleName)))
			usSpan.End()
			upSampleCh <- IGOSampleAdapter{su, m, subscribeCtx}
		case m.Subject == releaseTEMPOSamplesFilter:
			subscribeCtx, rtsSpan := tracer.Start(ctx, incomingReleaseTEMPOSamplesMsg)
			tempoSamples, err := protoUnMarshal(m.Data)
			if handleError(err, processingReleaseTEMPOSamplesErrMsg, rtsSpan) {
				break
			}
			rtsSpan.AddEvent(processingReleaseTEMPOSamplesSucMsg, trace.WithAttributes(attribute.String(TEMPOSampleNamesKey, buildStringFromTEMPOSamples(tempoSamples))))
			rtsSpan.AddEvent(handingOffTEMPOSamplesToRunLoopMsg, trace.WithAttributes(attribute.String(TEMPOSampleNamesKey, buildStringFromTEMPOSamples(tempoSamples))))
			rtsSpan.End()
			releaseTEMPOSamplesCh <- TEMPOSampleAdapter{tempoSamples, m, subscribeCtx}
		case m.Subject == updateTEMPOSampleFilter:
			subscribeCtx, utsSpan := tracer.Start(ctx, incomingUpTEMPOSamplesMsg)
			tempoSamples, err := protoUnMarshal(m.Data)
			if handleError(err, processingUpTEMPOSamplesErrMsg, utsSpan) {
				break
			}
			utsSpan.AddEvent(processingUpTEMPOSamplesSucMsg, trace.WithAttributes(attribute.String(TEMPOSampleNamesKey, buildStringFromTEMPOSamples(tempoSamples))))
			utsSpan.AddEvent(handingOffTEMPOSamplesToRunLoopMsg, trace.WithAttributes(attribute.String(TEMPOSampleNamesKey, buildStringFromTEMPOSamples(tempoSamples))))
			utsSpan.End()
			updateTEMPOSampleCh <- TEMPOSampleAdapter{tempoSamples, m, subscribeCtx}
		default:
			// not interested in message, Ack it so we don't get it again
			m.ProviderMsg.Ack()
		}
	})
	return err
}

func buildStringFromTEMPOSamples(tempoSamples []*st.TempoSample) string {
	var builder strings.Builder
	for lc, tempoSample := range tempoSamples {
		if lc > 0 {
			builder.WriteString(", ")
		}
		builder.WriteString(tempoSample.CmoSampleName)
	}
	return builder.String()
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

func protoUnMarshal(data []byte) ([]*st.TempoSample, error) {
	var tempoSamples st.TempoSampleUpdateMessage
	if err := proto.Unmarshal(data, &tempoSamples); err != nil {
		return tempoSamples.TempoSamples, err
	}
	return tempoSamples.TempoSamples, nil
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
