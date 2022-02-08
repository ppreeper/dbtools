package database

import (
	"context"
	"fmt"
	"time"

	_ "github.com/denisenkom/go-mssqldb" //mssql driver
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/jmoiron/sqlx"
	ec "github.com/ppreeper/dbtools/pkg/errcheck"
)

// Database struct contains sql pointer
type Database struct {
	Name     string `json:"name,omitempty"`
	Driver   string `json:"driver,omitempty"`
	Host     string `json:"host,omitempty"`
	Port     string `json:"port,omitempty"`
	Database string `json:"database,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	URI      string `json:"uri,omitempty"`
	// Log      *zap.SugaredLogger
	Drive  string `json:"drive,omitempty"`
	SubDir string `json:"subdir,omitempty"`
	*sqlx.DB
}

// Conn struct
type Conn struct {
	Source  *Database
	Dest    *Database
	SSchema string
	DSchema string
}

// OpenDatabase open database
func OpenDatabase(db Database) (*Database, error) {
	var err error
	db.GetURI()
	db.DB, err = sqlx.Open(db.Driver, db.URI)
	ec.FatalErr(err, "cannot open database")
	if err = db.Ping(); err != nil {
		ec.FatalErr(err, "cannot ping database")
	}
	return &db, err
}

// GenURI generate db uri string
func (db *Database) GetURI() {
	if db.Driver == "postgres" || db.Driver == "pgx" {
		port := "5432"
		if db.Port != "" {
			port = db.Port
		}
		db.URI = "postgres://" + db.Username + ":" + db.Password + "@" + db.Host + ":" + port + "/" + db.Database + "?sslmode=disable"
	}
	if db.Driver == "mssql" {
		db.URI = "server=" + db.Host + ";user id=" + db.Username + ";password=" + db.Password + ";database=" + db.Database + ";encrypt=disable;connection timeout=7200;keepAlive=30"
	}
}

//ExecProcedure executes stored procedure
func (db *Database) ExecProcedure(q string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Println(q)
	_, err := db.ExecContext(ctx, q)
	if err != nil {
		panic(err)
	}
}

func GenSubDir(db *Database) (subdir string) {
	subdir = fmt.Sprintf("%s:\\%s", db.Drive, db.SubDir)
	return
}
