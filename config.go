package smile_databricks_gateway

type Config struct {
	Hostname       string `docopt:"--host"`
	DBPort         int    `docopt:"--dbport"`
	DBToken        string `docopt:"--dbtoken"`
	DBTokenComment string `docopt:"--dbtokencomment"`
	HttpPath       string `docopt:"--path"`
	SMILESchema    string `docopt:"--smileschema"`
	RequestTable   string `docopt:"--requesttable"`
	SampleTable    string `docopt:"--sampletable"`
	SlackURL       string `docopt:"--slackurl"`
}

var TestConfig = Config{
	Hostname:       "",
	DBPort:         0,
	DBToken:        "",
	DBTokenComment: "",
	HttpPath:       "",
	SMILESchema:    "",
	RequestTable:   "",
	SampleTable:    "",
	SlackURL:       "",
}
