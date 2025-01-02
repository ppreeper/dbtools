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

	dbc "github.com/ppreeper/db_copy/database"
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
)

var log *zap.SugaredLogger

func checkErr(err error) {
	if err != nil {
		log.Errorw(err.Error())
	}
}

func fatalErr(err error) {
	if err != nil {
		log.Fatalw(err.Error())
	}
}

func dbOpen(d dbc.Database) *dbc.Database {
	db, err := dbc.OpenDatabase(dbc.Database{
		Name:     d.Name,
		Driver:   d.Driver,
		Host:     d.Host,
		Port:     d.Port,
		Database: d.Database,
		Schema:   d.Schema,
		Username: d.Username,
		Password: d.Password,
		PoolSize: d.PoolSize,
		Log:      log,
	})
	checkErr(err)
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
}

func main() {
	logName := "dbcopy.log"
	_, err := os.Stat(logName)
	if os.IsNotExist(err) {
		file, err := os.Create(logName)
		fatalErr(err)
		defer file.Close()
	}
	err = os.Truncate(logName, 0)
	checkErr(err)
	cfg := zap.NewProductionConfig()
	cfg.OutputPaths = []string{logName}
	logger, _ := cfg.Build()
	log = logger.Sugar()

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

	flag.Parse()

	log.Infow("start", "config", config)

	fmt.Println("Source: ", config.Source, "SSchemaName: ", config.SSchemaName, "Dest: ", config.Dest, "DSchemaName: ", config.DSchemaName)
	fmt.Println("Table: ", config.Table, "TableName: ", config.TableName)
	fmt.Println("View: ", config.View, "ViewName: ", config.ViewName)
	fmt.Println("Routine: ", config.Routine, "RoutineName: ", config.RoutineName)
	fmt.Println("Index: ", config.Index, "IndexName: ", config.IndexName)
	fmt.Println("All: ", config.All, "Link: ", config.Link, "Update: ", config.Update, "Debug: ", config.Debug)

	config.Filter = regexp.MustCompilePOSIX(config.FilterDef)

	// Config File
	userConfigDir, err := os.UserConfigDir()
	checkErr(err)
	var c Conf
	c.getConf(userConfigDir + "/db_copy/config.yml")

	src := c.getDB(config.Source)
	dst := c.getDB(config.Dest)

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

	var sSchemas []dbc.Schema
	if config.SSchemaName != "" {
		var s = dbc.Schema{Name: config.SSchemaName}
		sSchemas = append(sSchemas, s)
	} else {
		sSchemas, err = sdb.GetSchemas(config.Timeout)
		checkErr(err)
	}
	fmt.Println("schemas: ", sSchemas)

	//////////
	// dest DB
	//////////
	if config.Dest == "" {
		fmt.Println("No destination specified")
		return
	}

	var ddb *dbc.Database
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

		var data = dbc.Conn{
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

func getTables(config *Config, data *dbc.Conn) {
	var err error
	var sTables []dbc.Table

	if config.TableName != "" {
		sTables = []dbc.Table{{Name: config.TableName}}
	} else {
		sTables, err = data.Source.GetTables(data.SSchema, "BASE TABLE", config.Timeout)
		checkErr(err)
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

func getViews(config *Config, data *dbc.Conn) {
	var err error
	var sViews []dbc.ViewList

	if config.ViewName != "" {
		sViews = []dbc.ViewList{{Name: config.ViewName}}
	} else {
		sViews, err = data.Source.GetViews(data.SSchema, config.Timeout)
		checkErr(err)
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

func getRoutines(config *Config, data *dbc.Conn) {
	var err error
	var sRoutines []dbc.RoutineList

	if config.RoutineName != "" {
		sRoutines = []dbc.RoutineList{{Name: config.RoutineName}}
	} else {
		sRoutines, err = data.Source.GetRoutines(data.SSchema, config.Timeout)
		checkErr(err)
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

func getIndexes(config *Config, data *dbc.Conn) {
	var err error
	var sIndexes []dbc.IndexList

	if config.RoutineName != "" {
		sIndexes = []dbc.IndexList{{Name: config.IndexName}}
	} else {
		sIndexes, err = data.Source.GetIndexes(data.SSchema, config.Timeout)
		checkErr(err)
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

func backupTasker(config *Config, data *dbc.Conn, objects []string) {
	// fmt.Println("backupTasker")
	sem := make(chan int, config.JobCount)
	var wg sync.WaitGroup
	wg.Add(len(objects))
	bar := progressbar.Default(int64(len(objects)))
	for _, object := range objects {
		go func(sem chan int, wg *sync.WaitGroup, bar *progressbar.ProgressBar, config *Config, data *dbc.Conn, object string) {
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
						checkErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, dsql)
						checkErr(err)
						_, err = data.Dest.ExecContext(ctx, disql)
						checkErr(err)
						_, err = data.Dest.ExecContext(ctx, csql)
						checkErr(err)
						_, err = data.Dest.ExecContext(ctx, cisql)
						checkErr(err)
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
						checkErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, dsql)
						checkErr(err)
						_, err = data.Dest.ExecContext(ctx, csql)
						checkErr(err)
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
						checkErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, dsql)
						checkErr(err)
						_, err = data.Dest.ExecContext(ctx, csql)
						checkErr(err)
					}
				}
			}

			if config.View {
				vsql, err := data.Source.GetViewSchema(data.SSchema, object)
				checkErr(err)
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
						checkErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, csql)
						checkErr(err)
					}
				}
			}

			if config.Routine {
				rsql, err := data.Source.GetRoutineSchema(data.SSchema, object)
				checkErr(err)
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
						checkErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, csql)
						checkErr(err)
					}
				}
			}

			if config.Index {
				rsql, err := data.Source.GetIndexSchema(data.SSchema, object)
				checkErr(err)
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
						checkErr(err)
						err = ioutil.WriteFile(fn, []byte(csql), 0666)
						checkErr(err)
					} else {
						ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
						defer cancel()
						_, err := data.Dest.ExecContext(ctx, dsql)
						checkErr(err)
						_, err = data.Dest.ExecContext(ctx, csql)
						checkErr(err)
					}
				}
			}

			<-sem
		}(sem, &wg, bar, config, data, object)
	}
	wg.Wait()
}
