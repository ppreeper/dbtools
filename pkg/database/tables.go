package database

import (
	"context"
	"fmt"
	"time"

	ec "github.com/ppreeper/dbtools/pkg/errcheck"
)

//////////
// Tables
//////////

// Table list of tables
type Table struct {
	Name string `db:"TABLE_NAME"`
}

// GetTableList returns table list
func (db *Database) GetTables(schemaName, ttype string, timeout int) ([]Table, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	q := ""
	if db.Driver == "postgres" || db.Driver == "pgx" || db.Driver == "mssql" {
		q += "select TABLE_NAME \"TABLE_NAME\" "
		q += "from INFORMATION_SCHEMA.TABLES "
		q += "where TABLE_SCHEMA = '" + schemaName + "' "
		q += "and TABLE_TYPE = '" + ttype + "' "
		q += "order by TABLE_NAME"
	}
	tt := []Table{}
	if err := db.SelectContext(ctx, &tt, q); err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	return tt, nil
}

// GetTableSchema gets table definition
func (db *Database) GetTableSchema(conn *Conn, table string, timeout int) (sqld, sqlc, sqldi, sqlci string) {
	scols, err := db.GetColumnDetail(conn, table, false, timeout)

	ec.CheckErr(err)
	pcols, err := db.GetPKey(conn, table, timeout)
	ec.CheckErr(err)
	sqld, sqlc = db.GenTable(conn, table, scols, pcols)
	sqldi, sqlci = db.GenTableIndexSQL(conn, table)
	return
}

// GetForeignTableSchema gets table definition
func (db *Database) GetForeignTableSchema(conn *Conn, table string, timeout int) (sqld, sqlc string) {
	scols, err := db.GetColumnDetail(conn, table, false, timeout)
	ec.CheckErr(err)
	pcols, err := db.GetPKey(conn, table, timeout)
	ec.CheckErr(err)
	sqld, sqlc = db.GenLink(conn, table, scols, pcols)
	return
}

// GetUpdateTableSchema gets table definition
func (db *Database) GetUpdateTableSchema(conn *Conn, table string, timeout int) (sqld, sqlc string) {
	scols, err := db.GetColumnDetail(conn, table, false, timeout)
	ec.CheckErr(err)
	pcols, err := db.GetPKey(conn, table, timeout)
	ec.CheckErr(err)
	sqld, sqlc = db.GenUpdate(conn, table, scols, pcols)
	return
}
