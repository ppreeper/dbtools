package database

import (
	"context"
	"fmt"
	"time"
)

//////////
// Schemas
//////////

// Schema struct to hold schemas
type Schema struct {
	Name string `db:"SCHEMA_NAME"`
}

// GetSchemas returns schema list
func (db *Database) GetSchemas(timeout int) ([]Schema, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	q := ""
	if db.Driver == "postgres" || db.Driver == "pgx" {
		q = "select schema_name \"SCHEMA_NAME\" from information_schema.schemata where schema_name not in ('pg_catalog','information_schema') order by schema_name"
	} else if db.Driver == "mssql" {
		q = "select \"SCHEMA_NAME\" from INFORMATION_SCHEMA.SCHEMATA where SCHEMA_NAME not in ("
		q += "'INFORMATION_SCHEMA',"
		q += "'db_accessadmin',"
		q += "'db_backupoperator',"
		q += "'db_datareader',"
		q += "'db_datawriter',"
		q += "'db_ddladmin',"
		q += "'db_denydatareader',"
		q += "'db_denydatawriter',"
		q += "'db_owner',"
		q += "'db_securityadmin',"
		q += "'sys'"
		q += ") order by SCHEMA_NAME"
	}
	ss := []Schema{}
	if err := db.SelectContext(ctx, &ss, q); err != nil {
		return nil, fmt.Errorf("select: %v", err)
	}
	return ss, nil
}
