package database

import (
	"context"
	"fmt"
	"time"
)

// PKey struct
type PKey struct {
	PKey string `db:"CL"`
}

// GetPKey func
func (c *Conn) GetPKey(table string, timeout int) ([]PKey, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	q := ""
	switch c.Source.Driver {
	case "postgres", "pgx":
		q += "SELECT C.COLUMN_NAME \"CL\""
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C", c.Source.Database)
		q += fmt.Sprintf("\nJOIN %s.INFORMATION_SCHEMA.COLUMNS CLM ON", c.Source.Database)
		q += "\nC.TABLE_CATALOG = CLM.TABLE_CATALOG AND "
		q += "\nC.TABLE_SCHEMA = CLM.TABLE_SCHEMA AND "
		q += "\nC.TABLE_NAME = CLM.TABLE_NAME AND "
		q += "\nC.COLUMN_NAME = CLM.COLUMN_NAME"
		q += "\nWHERE C.TABLE_CATALOG = $1"
		q += "\nAND C.TABLE_SCHEMA = $2"
		q += "\nAND C.TABLE_NAME IN ($3)"
		q += "\nAND C.CONSTRAINT_NAME IN ("
		q += "\nSELECT CONSTRAINT_NAME"
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS C", c.Source.Database)
		q += "\nWHERE C.TABLE_CATALOG = $4"
		q += "\nAND C.TABLE_SCHEMA = $5"
		q += "\nAND CONSTRAINT_TYPE = 'PRIMARY KEY'"
		q += "\nAND C.TABLE_NAME IN ($6)"
		q += "\n)"
		q += "\nORDER BY CLM.ORDINAL_POSITION"
	case "mssql":
		q += "SELECT C.COLUMN_NAME \"CL\""
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C", c.Source.Database)
		q += fmt.Sprintf("\nJOIN %s.INFORMATION_SCHEMA.COLUMNS CLM ON", c.Source.Database)
		q += "\nC.TABLE_CATALOG = CLM.TABLE_CATALOG AND "
		q += "\nC.TABLE_SCHEMA = CLM.TABLE_SCHEMA AND "
		q += "\nC.TABLE_NAME = CLM.TABLE_NAME AND "
		q += "\nC.COLUMN_NAME = CLM.COLUMN_NAME"
		q += "\nWHERE C.TABLE_CATALOG = ?"
		q += "\nAND C.TABLE_SCHEMA = ?"
		q += "\nAND C.TABLE_NAME IN (?)"
		q += "\nAND C.CONSTRAINT_NAME IN ("
		q += "\nSELECT CONSTRAINT_NAME"
		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS C", c.Source.Database)
		q += "\nWHERE C.TABLE_CATALOG = ?"
		q += "\nAND C.TABLE_SCHEMA = ?"
		q += "\nAND CONSTRAINT_TYPE = 'PRIMARY KEY'"
		q += "\nAND C.TABLE_NAME IN (?)"
		q += "\n)"
		q += "\nORDER BY CLM.ORDINAL_POSITION"
	}
	var pkey []PKey
	if err := c.Source.SelectContext(ctx, &pkey, q, c.Source.Database, c.SSchema, table, c.Source.Database, c.SSchema, table); err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	return pkey, nil
}

// func (db *Database) GetPKey(conn *Conn, table string, timeout int) ([]PKey, error) {
// 	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
// 	defer cancel()
// 	q := ""
// 	if conn.Source.Driver == "postgres" || conn.Source.Driver == "pgx" || conn.Source.Driver == "mssql" {
// 		q += "SELECT C.COLUMN_NAME \"CL\""
// 		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE C", conn.Source.Database)
// 		q += fmt.Sprintf("\nJOIN %s.INFORMATION_SCHEMA.COLUMNS CLM ON", conn.Source.Database)
// 		q += "\nC.TABLE_CATALOG = CLM.TABLE_CATALOG AND "
// 		q += "\nC.TABLE_SCHEMA = CLM.TABLE_SCHEMA AND "
// 		q += "\nC.TABLE_NAME = CLM.TABLE_NAME AND "
// 		q += "\nC.COLUMN_NAME = CLM.COLUMN_NAME"
// 		q += fmt.Sprintf("\nWHERE C.TABLE_CATALOG = '%s'", conn.Source.Database)
// 		q += fmt.Sprintf("\nAND C.TABLE_SCHEMA = '%s'", conn.SSchema)
// 		q += fmt.Sprintf("\nAND C.TABLE_NAME IN ('%s')", table)
// 		q += "\nAND C.CONSTRAINT_NAME IN ("
// 		q += "\nSELECT CONSTRAINT_NAME"
// 		q += fmt.Sprintf("\nFROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS C", conn.Source.Database)
// 		q += fmt.Sprintf("\nWHERE C.TABLE_CATALOG = '%s'", conn.Source.Database)
// 		q += fmt.Sprintf("\nAND C.TABLE_SCHEMA = '%s'", conn.SSchema)
// 		q += "\nAND CONSTRAINT_TYPE = 'PRIMARY KEY'"
// 		q += fmt.Sprintf("\nAND C.TABLE_NAME IN ('%s')", table)
// 		q += "\n)"
// 		q += "\nORDER BY CLM.ORDINAL_POSITION"
// 	}
// 	var pkey []PKey
// 	if err := db.SelectContext(ctx, &pkey, q); err != nil {
// 		return nil, fmt.Errorf("select: %w", err)
// 	}
// 	return pkey, nil
// }
