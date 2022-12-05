package configfile

import (
	"os"

	ec "github.com/ppreeper/dbtools/pkg/errcheck"
	"gopkg.in/yaml.v2"
)

type Host struct {
	Hostname string `default:"localhost" json:"hostname"`
	Driver   string `default:"pgx" json:"driver"`
	Database string `default:"odoo" json:"database,omitempty"`
	Username string `default:"odoo" json:"username"`
	Password string `default:"odoo" json:"password"`
	Port     int    `default:"8069" json:"port,omitempty"`
}

func GetConf(configFile string) map[string]Host {
	yamlFile, err := os.ReadFile(configFile)
	ec.CheckErr(err)
	data := make(map[string]Host)
	err = yaml.Unmarshal(yamlFile, data)
	ec.CheckErr(err)
	return data
}
