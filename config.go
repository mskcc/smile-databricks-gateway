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
	Momrsf             string  `docopt:"--momrsf"`
	Momuef             string  `docopt:"--momuef"`
	OTELTracerHost     string  `docopt:"--tracerhost"`
	OTELTracerPort     int     `docopt:"--tracerport"`
	DatadogServiceName string  `docopt:"--ddservicename"`
	SlackURL           string  `docopt:"--slackurl"`
	SAML2AWSBin        string  `docopt:"--saml2aws"`
	SAMLProfile        string  `docopt:"--saml2profile"`
	SAMLRegion         string  `docopt:"--saml2region"`
	IGOAWSBucket       string  `docopt:"--igoawsbucket"`
	TEMPOAWSBucket     string  `docopt:"--tempoawsbucket"`
	AWSSessionDuration float64 `docopt:"--awssessionduration"`
}

var TestConfig = Config{
	SAML2AWSBin:        "",
	SAMLProfile:        "",
	SAMLRegion:         "",
	IGOAWSBucket:       "",
	AWSSessionDuration: 3600.0,
}
