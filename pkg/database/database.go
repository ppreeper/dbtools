package database

import (
	"context"
	"fmt"
	"time"

	_ "github.com/denisenkom/go-mssqldb" //mssql driver
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	ec "github.com/ppreeper/dbtools/pkg/errcheck"
)

//########
// Views
//########

// Database struct contains sql pointer
type Database struct {
	Name     string `json:"name,omitempty"`
	Hostname string `json:"hostname,omitempty"`
	Port     int    `json:"port,omitempty"`
	Driver   string `json:"driver,omitempty"`
	Database string `json:"database,omitempty"`
	Username string `json:"username,omitempty"`
	Password string `json:"password,omitempty"`
	URI      string `json:"uri,omitempty"`
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
		port := 5432
		if db.Port != 0 {
			port = db.Port
		}
		db.URI = fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=disable", db.Username, db.Password, db.Hostname, port, db.Database)
	}
	if db.Driver == "mssql" {
		db.URI = fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s;encrypt=disable;connection timeout=7200;keepAlive=30", db.Hostname, db.Username, db.Password, db.Database)
	}
}

// ExecProcedure executes stored procedure
func (db *Database) ExecProcedure(q string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	fmt.Println(q)
	_, err := db.ExecContext(ctx, q)
	if err != nil {
		panic(err)
	}
}
