package smile_databricks_gateway

type Config struct {
	DBHostname         string `docopt:"--host"`
	DBToken            string `docopt:"--dbtoken"`
	DBTokenComment     string `docopt:"--dbtokencomment"`
	DBHttpPath         string `docopt:"--dbhttppath"`
	DLTPipelineName    string `docopt:"--dltpipelinename"`
	MomUrl             string `docopt:"--momurl"`
	MomCert            string `docopt:"--momcert"`
	MomKey             string `docopt:"--momkey"`
	MomCons            string `docopt:"--momcons"`
	MomPw              string `docopt:"--mompw"`
	MomSub             string `docopt:"--momsub"`
	MomNrf             string `docopt:"--momnrf"`
	MomUrf             string `docopt:"--momurf"`
	MomUsf             string `docopt:"--momusf"`
	OTELTracerHost     string `docopt:"--tracerhost"`
	OTELTracerPort     int    `docopt:"--tracerport"`
	DatadogServiceName string `docopt:"--ddservicename"`
	SlackURL           string `docopt:"--slackurl"`
	SAML2AWSBin        string `docopt:"--saml2aws"`
	SAMLProfile        string `docopt:"--saml2profile"`
	SAMLRegion         string `docopt:"--saml2region"`
	AWSDestBucket      string `docopt:"--awsdestbucket"`
}

var TestConfig = Config{
	DBHostname:      "",
	DBToken:         "",
	DBTokenComment:  "",
	DBHttpPath:      "",
	DLTPipelineName: "",
	SAML2AWSBin:     "",
	SAMLProfile:     "",
	SAMLRegion:      "",
	AWSDestBucket:   "",
}
