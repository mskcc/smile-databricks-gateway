package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"sync"

	"github.com/docopt/docopt-go"
	sdg "github.com/mskcc/smile-databricks-gateway"
	"go.opentelemetry.io/otel"
)

const usage = `smile-databricks-gateway.

Usage:
  extract-datafeed -h | --help
  extract-datafeed --host=<hostname> --dbport=<port>
                   --dbtoken=<token>
                   --dbtokencomment=<comment>
                   --dbhttppath=<path>
                   --smileschema=<schema>
                   --requesttable=<requesttable>
                   --sampletable=<sampletable>
                   --momurl=<momurl>
                   --momcert=<momcert>
                   --momkey=<momkey>
                   --momcons=<momcons>
                   --mompw=<mompw>
                   --momsub=<momsub>
                   --momnrf=<momnrf>
                   --momurf=<momurf>
                   --momusf=<momusf>
                   --tracerhost=<hostname>
                   --tracerport=<port>
                   --ddservicename=<name>
                   --slackurl=<url>
                   --saml2aws=<saml2aws>
                   --saml2profile=<profile>
                   --saml2region=<region>
                   --awsdestbucket=<bucket>
Options:
  -h --help                     Show this screen.
  --host=<hostname>             Databricks hostname.
  --dbport=<port>               Databricks port.
  --dbtoken=<token>             Databricks personal access token.
  --dbtokencomment=<comment>    Databricks personal access token comment.
  --dbhttppath=<path>           The HTTP path to the Databricks SQL Warehouse.
  --smileschema=<schema>        The Databricks schema where the Extract status and release tables reside.
  --requesttable=<requesttable> The Databricks table where request records are stored.
  --sampletable=<sampletable>   The Databricks table where sample records are stored.
  --momurl=<momurl>             The messaging system URL.
  --momcert=<momcert>            The messaging system certificate.
  --momkey=<momkey>             The messaging system cert key.
  --momcons=<momcons>           The messaging system consumer (id)
  --mompw=<mompw>               The messaging system consumer pw.
  --momsub=<momsub>             The messaging system subject (topic).
  --momnrf<momnrf>              The messaging system new request topic filter.
  --momurf<momurf>              The messaging system update request topic filter.
  --momusf<momusf>              The messaging system update sample topic filter.
  --tracerhost=<hostname>       OTel Tracer hostname.
  --tracerport=<port>           OTel Tracer port.
  --ddservicename=<name>        Datadog service name.
  --slackurl=<url>              The URL to the slack channel for notification of new Extract project availability
  --saml2aws=<saml2aws>         The saml2aws script
  --saml2profile=<profile>      The aws creds profile
  --saml2region=<region>        The aws region
  --awsdestbucket=<bucket>      The dest bucket for smile json
`

func setupSignalListener(cancel context.CancelFunc, wg *sync.WaitGroup) {

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)

	go func(wg *sync.WaitGroup) {
		// block until signal is received
		s := <-c
		log.Printf("Got signal: '%s', shutting down SMILE Databricks Gateway...\n", s)
		cancel()
		wg.Wait()
	}(wg)
}

func handleError(err error, message string) {
	if err != nil {
		log.Fatalf("%s: %v", message, err)
	}
}

func main() {
	args, err := docopt.ParseDoc(usage)
	handleError(err, "Arguments cannot be parsed")

	var config sdg.Config
	err = args.Bind(&config)
	handleError(err, "Error binding arguments")

	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	setupSignalListener(cancel, &wg)

	shutdownTracer := sdg.InitTracerProvider(ctx, config.OTELTracerHost, config.OTELTracerPort, config.DatadogServiceName, "prod")
	defer shutdownTracer()
	tracer := otel.Tracer(config.DatadogServiceName + "-tracer")

	databricksService, close, err := sdg.NewDatabricksService(config.DBToken, config.DBTokenComment, config.DBHostname, config.HttpPath, config.SMILESchema, config.RequestTable, config.SampleTable, config.SlackURL, config.DBPort)
	handleError(err, "Databricks service cannot be created")
	defer close()

	awsS3Service := sdg.NewAWSS3Service(config.SAML2AWSBin, config.SAMLProfile, config.SAMLRegion, config.AWSDestBucket)

	// setup smile service
	smileService, err := sdg.NewSmileService(config.MomUrl, config.MomCert, config.MomKey, config.MomCons, config.MomPw, awsS3Service, databricksService)
	handleError(err, "SMILE Service cannot be created")
	if err := smileService.Run(ctx, config.MomCons, config.MomSub, config.MomNrf, config.MomUrf, config.MomUsf, tracer); err != nil {
		os.Exit(1)
	}
	log.Println("Exiting SMILE Databricks Gateway...")

}
