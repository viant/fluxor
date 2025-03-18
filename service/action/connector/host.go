package connector

type Host struct {
	URL         string `yaml:"URL" json:"url"`
	Credentials string
}
