package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/ppreeper/dbtools/pkg/configfile"
	"github.com/ppreeper/dbtools/pkg/database"
	"github.com/ppreeper/str"
	"github.com/schollz/progressbar/v3"

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

var logger *slog.Logger

func setupLogging(logName string) {
	// check for file existence
	_, err := os.Stat(logName)
	if os.IsNotExist(err) {
		file, err := os.Create(logName)
		ec.FatalErr(err)
		defer file.Close()
	}
	// if exists truncate file
	err = os.Truncate(logName, 0)
	ec.CheckErr(err)

	f, err := os.OpenFile(logName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	ec.FatalErr(err)
	logwriter := io.Writer(f)
	logger = slog.New(slog.NewTextHandler(logwriter, nil))
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
	// Config File
	userConfigDir, err := os.UserConfigDir()
	ec.CheckErr(err)

	// Flags
	var configFile string
	// init config struct
	config := Config{}

	flag.StringVar(&configFile, "c", userConfigDir+"/dbtools/config.yml", "config.yml")
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

	setupLogging(config.LogFile)

	HostMap := configfile.GetConf(configFile)

	// config options display
	logger.Info("start", "config", config)

	fmt.Println(str.RJustLen("Source:", 8), str.LJustLen(config.Source, 20), str.RJustLen("SSchemaName:", 13), str.LJustLen(config.SSchemaName, 20))
	fmt.Println(str.RJustLen("Dest:", 8), str.LJustLen(config.Dest, 20), str.RJustLen("DSchemaName:", 13), str.LJustLen(config.DSchemaName, 20))
	fmt.Println(str.RJustLen("Table:", 8), config.Table, str.RJustLen("TableName:", 13), config.TableName)
	fmt.Println(str.RJustLen("View:", 8), config.View, str.RJustLen("ViewName:", 13), config.ViewName)
	fmt.Println(str.RJustLen("Routine:", 8), config.Routine, str.RJustLen("RoutineName:", 13), config.RoutineName)
	fmt.Println(str.RJustLen("Index:", 8), config.Index, str.RJustLen("IndexName:", 13), config.IndexName)
	fmt.Println(str.RJustLen("All:", 8), config.All, str.RJustLen("Link:", 8), config.Link, str.RJustLen("Update:", 8), config.Update, str.RJustLen("Debug:", 8), config.Debug)

	config.Filter = regexp.MustCompilePOSIX(config.FilterDef)

	sdbConfig, ddbConfig := config.getDBConfigs(HostMap)

	config.checkParams()

	sdb, err := dbOpen(sdbConfig)
	ec.FatalErr(err)
	defer sdb.Close()

	if config.Dest == "file:" {
		ddbConfig = sdbConfig
	}
	ddb, err := dbOpen(ddbConfig)
	ec.FatalErr(err)
	defer ddb.Close()

	// =======
	// get schemas
	// =======

	sSchemas, err := sdb.GetSchemas(config.Timeout)
	ec.CheckErr(err)

	if config.SSchemaName != "" {
		// if SourceSchema specified then look it up
		s := database.Schema{}
		for _, v := range sSchemas {
			if v.Name == config.SSchemaName {
				s = database.Schema{Name: config.SSchemaName}
			}
		}
		if s.Name == "" {
			fmt.Println("no schema found")
			os.Exit(0)
		}
		sSchemas = []database.Schema{s}
	}
	fmt.Println("schemas: ", sSchemas)

	dSchemas, err := ddb.GetSchemas(config.Timeout)
	ec.CheckErr(err)

	if config.DSchemaName != "" {
		// if DSchemaName specified then look it up
		s := database.Schema{}
		for _, v := range dSchemas {
			if v.Name == config.DSchemaName {
				s = database.Schema{Name: config.DSchemaName}
			}
		}
		if s.Name == "" {
			fmt.Println("no schema found")
			os.Exit(0)
		}
		dSchemas = []database.Schema{s}
	}
	fmt.Println("schemas: ", dSchemas)

	for _, s := range sSchemas {
		fmt.Println(s)
		var DSchema string
		if config.Dest == "file:" {
			DSchema = s.Name
		} else if config.Dest != "file:" && config.DSchemaName == "" {
			DSchema = s.Name
		} else {
			DSchema = config.DSchemaName
		}

		data := database.Conn{
			Source:  sdb,
			Dest:    ddb,
			SSchema: s.Name,
			DSchema: DSchema,
		}
		fmt.Println(data.SSchema, data.DSchema)

		if config.Table || config.TableName != "" {
			fmt.Println("table:", s.Name)
			getTables(&config, &data)
		}
		// if config.View || config.ViewName != "" {
		// 	fmt.Println("view:", s.Name)
		// 	getViews(&config, &data)
		// }
		// if config.Routine || config.RoutineName != "" {
		// 	fmt.Println("routine:", s.Name)
		// 	getRoutines(&config, &data)
		// }
		// if config.Index || config.IndexName != "" {
		// 	fmt.Println("index:", s.Name)
		// 	getIndexes(&config, &data)
		// }
	}
}

func dbOpen(db configfile.Host) (*database.Database, error) {
	dbconn, err := database.OpenDatabase(database.Database{
		Hostname: db.Hostname,
		Port:     db.Port,
		Driver:   db.Driver,
		Database: db.Database,
		Username: db.Username,
		Password: db.Password,
	})
	return dbconn, err
}

func (config *Config) getDBConfigs(HostMap map[string]configfile.Host) (sourceDB, destDB configfile.Host) {
	// =======
	// source db
	// =======
	if config.Source == "" {
		fmt.Println("no source specified")
		os.Exit(0)
	}
	sourceDB = HostMap[config.Source]
	if sourceDB.Hostname == "" {
		fmt.Println("no source found")
		os.Exit(0)
	}

	// =======
	// dest DB
	// =======
	if config.Dest == "" {
		fmt.Println("no destination specified")
		os.Exit(0)
	}
	destDB = HostMap[config.Dest]
	if destDB.Hostname == "" {
		fmt.Println("no destination found")
		os.Exit(0)
	}
	return
}

func (config *Config) checkParams() {
	// =======
	// check all or table,view,routine
	// =======

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
		os.Exit(0)
	}
}

func getTables(config *Config, data *database.Conn) {
	var err error
	var sTables []database.Table

	if config.TableName != "" {
		sTables = []database.Table{{Name: config.TableName}}
	} else {
		sTables, err = data.GetTables(data.SSchema, "BASE TABLE", config.Timeout)
		ec.CheckErr(err)
	}

	var tbls []string
	for _, t := range sTables {
		if config.FilterDef == "" || !config.Filter.MatchString(t.Name) {
			tbls = append(tbls, t.Name)
		}
	}
	fmt.Println("sTables: ", len(sTables), "tables: ", len(tbls))

	cTable := config.Table
	cLink := config.Link
	cView := config.View
	cRoutine := config.Routine

	// config.Table = true
	config.View = false
	// config.Link = false
	// config.Routine = false

	if len(tbls) > 0 {
		fmt.Println("jobCount:", config.JobCount)
		fmt.Println(tbls)
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
		fmt.Println(object)
		go func(sem chan int, wg *sync.WaitGroup, bar *progressbar.ProgressBar, config *Config, data *database.Conn, object string) {
			defer bar.Add(1)
			defer wg.Done()
			sem <- 1

			if config.Table {
				// fundamentally wrong GetTableShemaa needs to be at the conn level
				// because the data returned depend on the source and dest drivers
				dsql, csql, disql, cisql := data.GetTableSchema(object, config.Timeout)
				if config.Debug {
					fmt.Println(dsql)
					fmt.Println(disql)
					fmt.Println(csql)
					fmt.Println(cisql)
				} else {
					if config.Dest == "file:" {
						fn := fmt.Sprintf("%s__t__%s.sql", data.DSchema, object)
						osql := fmt.Sprintf("%s\n%s\n%s\n%s", dsql, disql, csql, cisql)
						err := os.WriteFile(fn, []byte(osql), 0o666)
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

			// if config.Link && data.Dest.Driver == "postgres" {
			// 	dsql, csql := data.Source.GetForeignTableSchema(data, object, config.Timeout)
			// 	if config.Debug {
			// 		fmt.Println(dsql)
			// 		fmt.Println(csql)
			// 	} else {
			// 		if config.Dest == "file:" {
			// 			fn := fmt.Sprintf("%s__ft__%s.sql", data.DSchema, object)
			// 			osql := fmt.Sprintf("%s\n%s", dsql, csql)
			// 			err := os.WriteFile(fn, []byte(osql), 0o666)
			// 			ec.CheckErr(err)
			// 		} else {
			// 			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
			// 			defer cancel()
			// 			_, err := data.Dest.ExecContext(ctx, dsql)
			// 			ec.CheckErr(err)
			// 			_, err = data.Dest.ExecContext(ctx, csql)
			// 			ec.CheckErr(err)
			// 		}
			// 	}
			// }
			//
			// if config.Update {
			// 	dsql, csql := data.Source.GetUpdateTableSchema(data, object, config.Timeout)
			// 	if config.Debug {
			// 		fmt.Println(dsql)
			// 		fmt.Println(csql)
			// 	} else {
			// 		if config.Dest == "file:" {
			// 			fn := fmt.Sprintf("%s__upd_%s.sql", data.DSchema, object)
			// 			osql := fmt.Sprintf("%s\n%s", dsql, csql)
			// 			err := os.WriteFile(fn, []byte(osql), 0o666)
			// 			ec.CheckErr(err)
			// 		} else {
			// 			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
			// 			defer cancel()
			// 			_, err := data.Dest.ExecContext(ctx, dsql)
			// 			ec.CheckErr(err)
			// 			_, err = data.Dest.ExecContext(ctx, csql)
			// 			ec.CheckErr(err)
			// 		}
			// 	}
			// }
			//
			// if config.View {
			// 	vsql, err := data.Source.GetViewSchema(data.SSchema, object)
			// 	ec.CheckErr(err)
			// 	csql := ""
			// 	if data.Dest.Driver == "postgres" {
			// 		csql += fmt.Sprintf("CREATE OR REPLACE VIEW \"%s\".\"%s\" AS\n", data.DSchema, vsql.Name)
			// 	}
			// 	csql += vsql.Definition
			//
			// 	if config.Debug {
			// 		fmt.Println(csql)
			// 	} else {
			// 		if config.Dest == "file:" {
			// 			fn := fmt.Sprintf("%s__v__%s.sql", data.DSchema, object)
			// 			err := os.WriteFile(fn, []byte(csql), 0o666)
			// 			ec.CheckErr(err)
			// 		} else {
			// 			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
			// 			defer cancel()
			// 			_, err := data.Dest.ExecContext(ctx, csql)
			// 			ec.CheckErr(err)
			// 		}
			// 	}
			// }
			//
			// if config.Routine {
			// 	rsql, err := data.Source.GetRoutineSchema(data.SSchema, object)
			// 	ec.CheckErr(err)
			// 	csql := ""
			// 	if data.Dest.Driver == "postgres" {
			// 		csql = fmt.Sprintf("CREATE OR REPLACE %s \"%s\".\"%s\"", rsql.Type, data.DSchema, rsql.Name)
			// 		csql += fmt.Sprintf("() \nLANGUAGE %s\nAS $%s$", rsql.ExternalLanguage, strings.ToLower(rsql.Type))
			// 	}
			//
			// 	csql += rsql.Definition
			//
			// 	if data.Dest.Driver == "postgres" {
			// 		csql += fmt.Sprintf("$%s$\n;", strings.ToLower(rsql.Type))
			// 	}
			//
			// 	if config.Debug {
			// 		fmt.Println(csql)
			// 	} else {
			// 		if config.Dest == "file:" {
			// 			fn := fmt.Sprintf("%s__r__%s.sql", data.DSchema, object)
			// 			err := os.WriteFile(fn, []byte(csql), 0o666)
			// 			ec.CheckErr(err)
			// 		} else {
			// 			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
			// 			defer cancel()
			// 			_, err := data.Dest.ExecContext(ctx, csql)
			// 			ec.CheckErr(err)
			// 		}
			// 	}
			// }
			//
			// if config.Index {
			// 	rsql, err := data.Source.GetIndexSchema(data.SSchema, object)
			// 	ec.CheckErr(err)
			// 	idx := "\"" + strings.Replace(strings.Replace(rsql.Table+`_`+rsql.Columns+"_idx", "\"", "", -1), ",", "_", -1) + "\""
			// 	exists := ""
			// 	notexists := ""
			// 	if data.Dest.Driver == "postgres" {
			// 		exists = "IF EXISTS "
			// 		notexists = "IF NOT EXISTS "
			// 	}
			//
			// 	dsql := `DROP INDEX ` + exists + `"` + rsql.Schema + `".` + idx + `;`
			// 	csql := `CREATE INDEX ` + notexists + `` + idx + ` ON "` + rsql.Schema + `"."` + rsql.Table + `" (` + rsql.Columns + `);`
			//
			// 	if config.Debug {
			// 		fmt.Println(dsql)
			// 		fmt.Println(csql)
			// 	} else {
			// 		if config.Dest == "file:" {
			// 			fn := fmt.Sprintf("%s__i__%s.sql", data.DSchema, object)
			// 			err := os.WriteFile(fn, []byte(dsql), 0o666)
			// 			ec.CheckErr(err)
			// 			err = os.WriteFile(fn, []byte(csql), 0o666)
			// 			ec.CheckErr(err)
			// 		} else {
			// 			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(config.Timeout)*time.Second)
			// 			defer cancel()
			// 			_, err := data.Dest.ExecContext(ctx, dsql)
			// 			ec.CheckErr(err)
			// 			_, err = data.Dest.ExecContext(ctx, csql)
			// 			ec.CheckErr(err)
			// 		}
			// 	}
			// }

			<-sem
		}(sem, &wg, bar, config, data, object)
	}
	wg.Wait()
}
