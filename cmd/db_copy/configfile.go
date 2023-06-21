package main

import (
	"os"

	dbc "github.com/ppreeper/db_copy/database"
	"gopkg.in/yaml.v2"
)

// Conf config structure
type Host struct {
	Hostname string `default:"localhost" json:"hostname"`
	Driver   string `default:"pgx" json:"driver"`
	Database string `default:"odoo" json:"database,omitempty"`
	Username string `default:"odoo" json:"username"`
	Password string `default:"odoo" json:"password"`
	Port     int    `default:"8069" json:"port,omitempty"`
}

// Conf array of Dbase
type Conf struct {
	Dbases []dbc.Database `json:"dbases,omitempty"`
}

func GetConf(configFile string) map[string]Host {
	yamlFile, err := os.ReadFile(configFile)
	checkErr(err)
	data := make(map[string]Host)
	err = yaml.Unmarshal(yamlFile, data)
	checkErr(err)
	return data
}

// func (c *Conf) getDB(name string) (d dbc.Database) {
// 	for _, v := range c.Dbases {
// 		if v.Name == name {
// 			d = v
// 		}
// 	}
// 	return
// }
