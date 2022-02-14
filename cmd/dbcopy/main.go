package main

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/ppreeper/dbtools/pkg/configfile"
	"github.com/ppreeper/dbtools/pkg/database"
	"github.com/ppreeper/pad"
	"github.com/schollz/progressbar/v3"

	"go.uber.org/zap"
	// _ "github.com/denisenkom/go-mssqldb"
	// _ "github.com/fajran/go-monetdb" //Monet
	// _ "github.com/jmoiron/sqlx"
	// _ "github.com/lib/pq"
	// _ "github.com/mattn/go-sqlite3" //sqlite3
	// _ "gopkg.in/mgo.v2" //Mongo
	// _ "github.com/go-sql-driver/mysql/" //MySql
	// _ "github.com/nakagami/firebirdsql" //Firebird Sql
	// _ "bitbucket.org/phiggins/db2cli" //DB2
	ec "github.com/ppreeper/dbtools/pkg/errcheck"
)

var log *zap.SugaredLogger

func dbOpen(d database.Database) *database.Database {
	db, err := database.OpenDatabase(database.Database{
		Name:     d.Name,
		Driver:   d.Driver,
		Host:     d.Host,
		Port:     d.Port,
		Database: d.Database,
		Username: d.Username,
		Password: d.Password,
	})
	ec.CheckErr(err)
	return db
}

type Config struct {
	Source      string
	Dest        string
	SSchemaName string
	DSchemaName string
	TableName   string
	Table       bool
	ViewName    string
	View        bool
	RoutineName string
	Routine     bool
	IndexName   string
	Index       bool
	FilterDef   string
	Filter      *regexp.Regexp
	JobCount    int
	Link        bool
	Debug       bool
	Timeout     int
	Update      bool
	All         bool
	LogFile     string
}

func main() {
	// init config struct
	config := Config{
		JobCount: 8,
	}

	// Flags
	flag.StringVar(&config.Source, "source", "", "source database")
	flag.StringVar(&config.SSchemaName, "ss", "", "source schema")
	flag.StringVar(&config.Dest, "dest", "", "destination database or file:")
	flag.StringVar(&config.DSchemaName, "ds", "", "dest schema")

	flag.StringVar(&config.TableName, "table", "", "specific table")
	flag.BoolVar(&config.Table, "t", false, "gen table sql")

	flag.StringVar(&config.ViewName, "view", "", "specific view")
	flag.BoolVar(&config.View, "v", false, "gen view sql")

	flag.StringVar(&config.RoutineName, "routine", "", "specific routine")
	flag.BoolVar(&config.Routine, "r", false, "gen routine sql")

	flag.StringVar(&config.IndexName, "index", "", "specific index")
	flag.BoolVar(&config.Index, "i", false, "gen index sql")

	flag.BoolVar(&config.All, "all", false, "all tables")

	flag.BoolVar(&config.Link, "l", false, "gen table link sql")
	flag.BoolVar(&config.Update, "u", false, "gen update procedure")

	flag.StringVar(&config.FilterDef, "f", "", "regex filter")

	flag.BoolVar(&config.Debug, "n", false, "no-op debug")
	flag.IntVar(&config.JobCount, "j", 8, "job count")
	flag.IntVar(&config.Timeout, "timeout", 10, "query timeout")
	flag.StringVar(&config.LogFile, "logfile", "dbcopy.log", "log file")

	flag.Parse()

	// Config File
	userConfigDir, err := os.UserConfigDir()
	ec.CheckErr(err)
	userDir, err := os.UserHomeDir()
	ec.CheckErr(err)
	var c configfile.Conf
	c.GetConf(userConfigDir + "/dbtools/config.yml")

	// logging
	logName := userDir + "/" + config.LogFile
	_, err = os.Stat(logName)
	if os.IsNotExist(err) {
		file, err := os.Create(logName)
		ec.FatalErr(err)
		defer file.Close()
	}
	err = os.Truncate(logName, 0)
	ec.CheckErr(err)
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{logName}
	logger, _ := cfg.Build()
	log = logger.Sugar()

	// config options display
	log.Infow("start", "config", config)

	fmt.Println(pad.RJustLen("Source:", 8), pad.LJustLen(config.Source, 20), pad.RJustLen("SSchemaName:", 13), pad.LJustLen(config.SSchemaName, 20))
	fmt.Println(pad.RJustLen("Dest:", 8), pad.LJustLen(config.Dest, 20), pad.RJustLen("DSchemaName:", 13), pad.LJustLen(config.DSchemaName, 20))
	fmt.Println(pad.RJustLen("Table:", 8), config.Table, pad.RJustLen("TableName:", 13), config.TableName)
	fmt.Println(pad.RJustLen("View:", 8), config.View, pad.RJustLen("ViewName:", 13), config.ViewName)
	fmt.Println(pad.RJustLen("Routine:", 8), config.Routine, pad.RJustLen("RoutineName:", 13), config.RoutineName)
	fmt.Println(pad.RJustLen("Index:", 8), config.Index, pad.RJustLen("IndexName:", 13), config.IndexName)
	fmt.Println(pad.RJustLen("All:", 8), config.All, pad.RJustLen("Link:", 8), config.Link, pad.RJustLen("Update:", 8), config.Update, pad.RJustLen("Debug:", 8), config.Debug)

	config.Filter = regexp.MustCompilePOSIX(config.FilterDef)

	// get database connections
	src, err := c.GetDB(config.Source)
	ec.FatalErr(err)
	dst, err := c.GetDB(config.Dest)
	ec.FatalErr(err)

	//////////
	// check all or table,view,routine
	//////////

	if config.All {
		config.Table = true
		config.View = true
		config.Routine = true
	}

	if (!config.Table && config.TableName == "") &&
		(!config.View && config.ViewName == "") &&
		(!config.Routine && config.RoutineName == "") &&
		(!config.Index && config.IndexName == "") {
		fmt.Println("table, view, routine, index, flags have to be selected")
		return
	}

	//////////
	// Source DB
	//////////
	if config.Source == "" {
		fmt.Println("No source specified")
		return
	}

	//////////
	// get schemas
	//////////

	// open source database connection
	sdb := dbOpen(src)
	defer sdb.Close()

	var sSchemas []database.Schema
	if config.SSchemaName != "" {
		var s = database.Schema{Name: config.SSchemaName}
		sSchemas = append(sSchemas, s)
	} else {
		sSchemas, err = sdb.GetSchemas(config.Timeout)
		ec.CheckErr(err)
	}
	fmt.Println("schemas: ", sSchemas)

	//////////
	// dest DB
	//////////
	if config.Dest == "" {
		fmt.Println("No destination specified")
		return
	}

	var ddb *database.Database
	var DSchema string

	if config.Dest == "file:" {
		ddb = sdb
	} else {
		ddb = dbOpen(dst)
	}
	defer ddb.Close()

	for _, s := range sSchemas {
		if config.Dest == "file:" {
			DSchema = s.Name
		} else if config.Dest != "file:" && config.DSchemaName == "" {
			DSchema = s.Name
		} else {
			DSchema = config.DSchemaName
		}

		var data = database.Conn{
			Source:  sdb,
			Dest:    ddb,
			SSchema: s.Name,
			DSchema: DSchema,
		}

		if config.Table || config.TableName != "" {
			fmt.Println("table:", s.Name)
			getTables(&config, &data)
		}
		if config.View || config.ViewName != "" {
			fmt.Println("view:", s.Name)
			getViews(&config, &data)
		}
		if config.Routine || config.RoutineName != "" {
			fmt.Println("routine:", s.Name)
			getRoutines(&config, &data)
		}
		if config.Index || config.IndexName != "" {
			fmt.Println("index:", s.Name)
			getIndexes(&config, &data)
		}
	}
}

func getTables(config *Config, data *database.Conn) {
	var err error
	var sTables []database.Table

	if config.TableName != "" {
		sTables = []database.Table{{Name: config.TableName}}
	} else {
		sTables, err = data.Source.GetTables(data.SSchema, "BASE TABLE", config.Timeout)
		ec.CheckErr(err)
	}
	// fmt.Println("tables: ", len(sTables))

	var tbls []string
	for _, t := range sTables {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			tbls = append(tbls, t.Name)
		}
	}
	// fmt.Println("sTables: ", len(sTables), "tables: ", len(tbls))

	cTable := config.Table
	cLink := config.Link
	cView := config.View
	cRoutine := config.Routine

	// config.Table = true
	config.View = false
	// config.Link = false
	// config.Routine = false

	if len(tbls) > 0 {
		// fmt.Println("jobCount:", config.JobCount)
		backupTasker(config, data, tbls)
	}

	config.Table = cTable
	config.Link = cLink
	config.View = cView
	config.Routine = cRoutine

}

func getViews(config *Config, data *database.Conn) {
	var err error
	var sViews []database.ViewList

	if config.ViewName != "" {
		sViews = []database.ViewList{{Name: config.ViewName}}
	} else {
		sViews, err = data.Source.GetViews(data.SSchema, config.Timeout)
		ec.CheckErr(err)
	}
	// fmt.Println("views: ", len(sViews))

	var views []string
	for _, t := range sViews {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			views = append(views, t.Name)
		}
	}
	// fmt.Println("sViews: ", len(sViews), "views: ", len(views))

	cTable := config.Table
	cLink := config.Link
	cView := config.View
	cRoutine := config.Routine

	config.Table = false
	config.View = true
	config.Link = false
	config.Routine = false

	if len(views) > 0 {
		// fmt.Println("jobCount:", config.JobCount)
		backupTasker(config, data, views)
	}

	config.Table = cTable
	config.Link = cLink
	config.View = cView
	config.Routine = cRoutine

}

func getRoutines(config *Config, data *database.Conn) {
	var err error
	var sRoutines []database.RoutineList

	if config.RoutineName != "" {
		sRoutines = []database.RoutineList{{Name: config.RoutineName}}
	} else {
		sRoutines, err = data.Source.GetRoutines(data.SSchema, config.Timeout)
		ec.CheckErr(err)
	}
	// fmt.Println("routines: ", len(sRoutines))

	var routines []string
	for _, t := range sRoutines {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			routines = append(routines, t.Name)
		}
	}
	// fmt.Println("sRoutines: ", len(sRoutines), "tables: ", len(routines))

	cTable := config.Table
	cLink := config.Link
	cView := config.View
	cRoutine := config.Routine

	config.Table = false
	config.View = false
	config.Link = false
	config.Routine = true

	if len(routines) > 0 {
		// fmt.Println("jobCount:", config.JobCount)
		backupTasker(config, data, routines)
	}

	config.Table = cTable
	config.Link = cLink
	config.View = cView
	config.Routine = cRoutine
}

func getIndexes(config *Config, data *database.Conn) {
	var err error
	var sIndexes []database.IndexList

	if config.RoutineName != "" {
		sIndexes = []database.IndexList{{Name: config.IndexName}}
	} else {
		sIndexes, err = data.Source.GetIndexes(data.SSchema, config.Timeout)
		ec.CheckErr(err)
	}
	// fmt.Println("routines: ", len(sIndexes))

	var indexes []string
	for _, t := range sIndexes {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			indexes = append(indexes, t.Name)
		}
	}
	// fmt.Println("sIndexes: ", len(sIndexes), "tables: ", len(indexes))

	if len(indexes) > 0 {
		// fmt.Println("jobCount:", config.JobCount)
		backupTasker(config, data, indexes)
	}
}

func backupTasker(config *Config, data *database.Conn, objects []string) {
	// fmt.Println("backupTasker")
	sem := make(chan int, config.JobCount)
	var wg sync.WaitGroup
	wg.Add(len(objects))
	bar := progressbar.Default(int64(len(objects)))
	for _, object := range objects {
		go func(sem chan int, wg *sync.WaitGroup, bar *progressbar.ProgressBar, config *Config, data *database.Conn, object string) {
			defer bar.Add(1)
			defer wg.Done()
			sem <- 1

			if config.Table {
				dsql, csql, disql, cisql := data.Source.GetTableSchema(data, object, config.Timeout)
				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(disql)
					fmt.Println(csql)
					fmt.Println(cisql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__t__%s.sql", data.DSchema, object)
						osql := fmt.Sprintf("%s\n%s\n%s\n%s", dsql, disql, csql, cisql)
						err := ioutil.WriteFile(fn, []byte(osql), 0666)
						ec.CheckErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, dsql)
						ec.CheckErr(err)
						_, err = data.Dest.ExecContext(ctx, disql)
						ec.CheckErr(err)
						_, err = data.Dest.ExecContext(ctx, csql)
						ec.CheckErr(err)
						_, err = data.Dest.ExecContext(ctx, cisql)
						ec.CheckErr(err)
					}
				}

			}

			if config.Link && data.Dest.Driver == "postgres" {
				dsql, csql := data.Source.GetForeignTableSchema(data, object, config.Timeout)
				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__ft__%s.sql", data.DSchema, object)
						osql := fmt.Sprintf("%s\n%s", dsql, csql)
						err := ioutil.WriteFile(fn, []byte(osql), 0666)
						ec.CheckErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, dsql)
						ec.CheckErr(err)
						_, err = data.Dest.ExecContext(ctx, csql)
						ec.CheckErr(err)
					}
				}
			}

			if config.Update {
				dsql, csql := data.Source.GetUpdateTableSchema(data, object, config.Timeout)
				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__upd_%s.sql", data.DSchema, object)
						osql := fmt.Sprintf("%s\n%s", dsql, csql)
						err := ioutil.WriteFile(fn, []byte(osql), 0666)
						ec.CheckErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, dsql)
						ec.CheckErr(err)
						_, err = data.Dest.ExecContext(ctx, csql)
						ec.CheckErr(err)
					}
				}
			}

			if config.View {
				vsql, err := data.Source.GetViewSchema(data.SSchema, object)
				ec.CheckErr(err)
				csql := ""
				if data.Dest.Driver == "postgres" {
					csql += fmt.Sprintf("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS\n", data.DSchema, vsql.Name)
				}
				csql += vsql.Definition

				if config.Debug {
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__v__%s.sql", data.DSchema, object)
						err := ioutil.WriteFile(fn, []byte(csql), 0666)
						ec.CheckErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, csql)
						ec.CheckErr(err)
					}
				}
			}

			if config.Routine {
				rsql, err := data.Source.GetRoutineSchema(data.SSchema, object)
				ec.CheckErr(err)
				csql := ""
				if data.Dest.Driver == "postgres" {
					csql = fmt.Sprintf("CREATE OR REPLACE %s \"%s\".\"%s\"", rsql.Type, data.DSchema, rsql.Name)
					csql += fmt.Sprintf("() \nLANGUAGE %s\nAS $%s$", rsql.ExternalLanguage, strings.ToLower(rsql.Type))
				}

				csql += rsql.Definition

				if data.Dest.Driver == "postgres" {
					csql += fmt.Sprintf("$%s$\n;", strings.ToLower(rsql.Type))
				}

				if config.Debug {
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__r__%s.sql", data.DSchema, object)
						err := ioutil.WriteFile(fn, []byte(csql), 0666)
						ec.CheckErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, csql)
						ec.CheckErr(err)
					}
				}
			}

			if config.Index {
				rsql, err := data.Source.GetIndexSchema(data.SSchema, object)
				ec.CheckErr(err)
				idx := "\"" + strings.Replace(strings.Replace(rsql.Table+`_`+rsql.Columns+"_idx", "\"", "", -1), ",", "_", -1) + "\""
				exists := ""
				notexists := ""
				if data.Dest.Driver == "postgres" {
					exists = "IF EXISTS "
					notexists = "IF NOT EXISTS "
				}

				dsql := `DROP INDEX ` + exists + `"` + rsql.Schema + `".` + idx + `;`
				csql := `CREATE INDEX ` + notexists + `` + idx + ` ON "` + rsql.Schema + `"."` + rsql.Table + `" (` + rsql.Columns + `);`

				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(csql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__i__%s.sql", data.DSchema, object)
						err := ioutil.WriteFile(fn, []byte(dsql), 0666)
						ec.CheckErr(err)
						err = ioutil.WriteFile(fn, []byte(csql), 0666)
						ec.CheckErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, dsql)
						ec.CheckErr(err)
						_, err = data.Dest.ExecContext(ctx, csql)
						ec.CheckErr(err)
					}
				}
			}

			<-sem
		}(sem, &wg, bar, config, data, object)
	}
	wg.Wait()
}
