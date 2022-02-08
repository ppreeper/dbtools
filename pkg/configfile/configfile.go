package configfile

import (
	"fmt"
	"os"

	"github.com/ppreeper/dbtools/pkg/database"
	ec "github.com/ppreeper/dbtools/pkg/errcheck"
	"gopkg.in/yaml.v2"
)

// Conf array of Dbase
type Conf struct {
	Databases []database.Database `json:"databases,omitempty"`
}

func (c *Conf) GetConf(configFile string) (*Conf, error) {
	yamlFile, err := os.ReadFile(configFile)
	ec.CheckErr(err)
	err = yaml.Unmarshal(yamlFile, c)
	ec.CheckErr(err)
	return c, err
}

func (c *Conf) GetDB(name string) (d database.Database, err error) {
	for _, v := range c.Databases {
		if v.Name == name {
			return v, err
		}
	}
	return database.Database{}, fmt.Errorf("no database found")
}
