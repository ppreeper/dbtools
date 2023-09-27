package database

import (
	"context"
	"fmt"
	"time"

	ec "github.com/ppreeper/dbtools/pkg/errcheck"
)

//########
// Tables
//########

// Table list of tables
type Table struct {
	Name string `db:"TABLE_NAME"`
}

// GetTableList returns table list
func (c *Conn) GetTables(schemaName, ttype string, timeout int) ([]Table, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	q := ""
	switch c.Source.Driver {
	case "postgres", "pgx":
		q += `SELECT TABLE_NAME "TABLE_NAME" 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = $1 AND TABLE_TYPE = $2 
		ORDER BY TABLE_NAME`
	case "mssql":
		q += `SELECT TABLE_NAME "TABLE_NAME" 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = ?
		ORDER BY TABLE_NAME`
	}
	tt := []Table{}
	if err := c.Source.SelectContext(ctx, &tt, q, schemaName, ttype); err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	return tt, nil
}

// GetTableSchema gets table definition
func (c *Conn) GetTableSchema(table string, timeout int) (sqld, sqlc, sqldi, sqlci string) {
	scols, err := c.GetColumnDetail(table, timeout)
	ec.CheckErr(err)
	pcols, err := c.GetPKey(table, timeout)
	ec.CheckErr(err)
	sqld, sqlc = c.GenTables(table, scols, pcols)
	sqldi, sqlci = c.GenTableIndexSQL(table)
	return
}

// GetForeignTableSchema gets table definition
func (c *Conn) GetForeignTableSchema(table string, timeout int) (sqld, sqlc string) {
	scols, err := c.GetColumnDetail(table, timeout)
	ec.CheckErr(err)
	pcols, err := c.GetPKey(table, timeout)
	ec.CheckErr(err)
	sqld, sqlc = c.GenLink(table, scols, pcols)
	return
}

// GetUpdateTableSchema gets table definition
func (c *Conn) GetUpdateTableSchema(table string, timeout int) (sqld, sqlc string) {
	scols, err := c.GetColumnDetail(table, timeout)
	ec.CheckErr(err)
	pcols, err := c.GetPKey(table, timeout)
	ec.CheckErr(err)
	sqld, sqlc = c.GenUpdate(table, scols, pcols)
	return
}
