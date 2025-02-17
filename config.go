package smile_databricks_gateway

type Config struct {
	MomUrl             string  `docopt:"--momurl"`
	MomCert            string  `docopt:"--momcert"`
	MomKey             string  `docopt:"--momkey"`
	MomCons            string  `docopt:"--momcons"`
	MomPw              string  `docopt:"--mompw"`
	MomSub             string  `docopt:"--momsub"`
	MomNrf             string  `docopt:"--momnrf"`
	MomUrf             string  `docopt:"--momurf"`
	MomUsf             string  `docopt:"--momusf"`
	OTELTracerHost     string  `docopt:"--tracerhost"`
	OTELTracerPort     int     `docopt:"--tracerport"`
	DatadogServiceName string  `docopt:"--ddservicename"`
	SlackURL           string  `docopt:"--slackurl"`
	SAML2AWSBin        string  `docopt:"--saml2aws"`
	SAMLProfile        string  `docopt:"--saml2profile"`
	SAMLRegion         string  `docopt:"--saml2region"`
	AWSDestBucket      string  `docopt:"--awsdestbucket"`
	AWSSessionDuration float64 `docopt:"--awssessionduration"`
}

var TestConfig = Config{
	SAML2AWSBin:     "",
	SAMLProfile:     "",
	SAMLRegion:      "",
	AWSDestBucket:   "",
	AWSSessionDuration: 3600.0,
}
