package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/ppreeper/dbtools/pkg/configfile"
	"github.com/ppreeper/dbtools/pkg/database"
	ec "github.com/ppreeper/dbtools/pkg/errcheck"
	"github.com/ppreeper/str"
)

func main() {
	// Config File
	userConfigDir, err := os.UserConfigDir()
	ec.CheckErr(err)
	HostMap := configfile.GetConf(userConfigDir + "/dbtools/config.yml")

	// flags
	var dbase, stmt string
	var timer bool

	flag.StringVar(&dbase, "db", "", "database")
	flag.StringVar(&stmt, "q", "", "sql query")
	flag.BoolVar(&timer, "t", false, "sql timer")
	flag.Parse()

	if dbase == "" {
		fmt.Println("no database specified")
		os.Exit(1)
	}
	src := HostMap[dbase]
	fmt.Println(src)
	if stmt == "" {
		fmt.Println("no query specified")
		os.Exit(2)
	}

	// connect to source database
	// open database connection
	// sdb, err := database.OpenDatabase(src)
	// ec.CheckErr(err)
	// defer func() {
	// 	if err := sdb.Close(); err != nil {
	// 		ec.CheckErr(err)
	// 	}
	// }()
	// ec.CheckErr(err)

	// start := time.Now()
	// colNames, dataSet := queryData(sdb, stmt)
	// elapsed := time.Since(start)

	// printData(&colNames, &dataSet)
	// if timer {
	// 	fmt.Printf("query: %s\ntime: %s\n", stmt, elapsed.String())
	// }
}

func queryData(sdb *database.Database, stmt string) (colNames []string, dataSet []interface{}) {
	rows, err := sdb.DB.Queryx(stmt)
	ec.FatalErr(err)
	defer rows.Close()

	colNames, err = rows.Columns()
	ec.CheckErr(err)
	cols := make([]interface{}, len(colNames))
	colPtrs := make([]interface{}, len(colNames))
	for i := 0; i < len(colNames); i++ {
		colPtrs[i] = &cols[i]
	}

	for rows.Next() {
		var rowMap = make(map[string]interface{})
		err = rows.Scan(colPtrs...)
		ec.FatalErr(err)
		for i, col := range cols {
			rowMap[colNames[i]] = col
		}
		dataSet = append(dataSet, rowMap)
	}
	return
}

func printData(colNames *[]string, dataSet *[]interface{}) {
	colLens := make([]int, len(*colNames))
	for k, v := range *colNames {
		if len(v) > colLens[k] {
			colLens[k] = len(v)
		}
	}
	// get maximum field lengths
	for _, v := range *dataSet {
		for k, c := range *colNames {
			vs := fmt.Sprintf("%v", v.(map[string]interface{})[c])
			if len(vs) > colLens[k] {
				colLens[k] = len(vs)
			}
		}
	}
	// print headers
	hdr := ""
	for k, v := range *colNames {
		hdr += fmt.Sprintf("%v", str.LJustLen(v, colLens[k]))
		if k < len(*colNames)-1 {
			hdr += ";"
			// fmt.Printf(";")
		}
	}
	fmt.Println(hdr)
	// print line items
	for _, v := range *dataSet {
		line := ""
		for k, c := range *colNames {
			val := v.(map[string]interface{})[c]
			vs := ""
			switch val.(type) {
			case string:
				vs = fmt.Sprintf("%s", val)
			case float64:
				vs = fmt.Sprintf("%f", val)
			default:
				vs = fmt.Sprintf("%v", val)
			}
			line += str.LJustLen(vs, colLens[k])
			if k < len(*colNames)-1 {
				line += ";"
			}
		}
		fmt.Println(line)
	}
}
