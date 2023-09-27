package database

import (
	"fmt"
	"strings"

	ec "github.com/ppreeper/dbtools/pkg/errcheck"
)

//########
// Generate
//########

// GenTable generate table creation
func (c *Conn) GenTables(table string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	clen := len(cols)
	plen := len(pkey)
	switch c.Dest.Driver {
	case "postgres", "pgx":
		sqld += fmt.Sprintf("\nDROP TABLE IF EXISTS \"%s\".\"%s\" CASCADE;", c.DSchema, table)
		sqlc += fmt.Sprintf("\nCREATE TABLE IF NOT EXISTS \"%s\".\"%s\" (\n", c.DSchema, table)
		for k, c := range cols {
			cdefault := ""
			if c.ColumnDefault != "" {
				cdefault += " DEFAULT " + strings.ReplaceAll(c.ColumnDefault, "getdate()", "CURRENT_TIMESTAMP")
			}
			sqlc += fmt.Sprintf("\"%s\" %s %s%s", c.ColumnName, c.DataType, c.IsNullable, cdefault)
			if k == clen-1 {
				if plen > 0 {
					sqlc += ",\n"
					sqlc += "PRIMARY KEY ("
					for v, p := range pkey {
						if v == plen-1 {
							sqlc += "\"" + p.PKey + "\""
						} else {
							sqlc += "\"" + p.PKey + "\","
						}
					}
					sqlc += ")" + "\n"
				} else {
					sqlc += "\n"
				}
			} else {
				sqlc += ",\n"
			}
		}
		sqlc += ");\n"
	case "mssql":
		sqld += fmt.Sprintf("\nDROP TABLE \"%s\".\"%s\";", c.DSchema, table)
		sqlc += fmt.Sprintf("\nCREATE TABLE \"%s\".\"%s\" (\n", c.DSchema, table)
		for k, c := range cols {
			if k == clen-1 {
				if plen > 0 {
					sqlc += fmt.Sprintf("%s,\n", c.ColumnName)
					sqlc += "PRIMARY KEY ("
					for v, p := range pkey {
						if v == plen-1 {
							sqlc += fmt.Sprintf("\"%s\"", p.PKey)
						} else {
							sqlc += fmt.Sprintf("\"%s\",", p.PKey)
						}
					}
					sqlc += ")\n"
				} else {
					// q += c.Column + "\n"
					sqlc += fmt.Sprintf("%s\n", c.ColumnName)
				}
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.ColumnName)
			}
		}
		sqlc += ")\n"
	default:
		sqld = ""
		sqlc = ""
	}
	return
}

// GenTableIndexSQL generate table index sql
func (c *Conn) GenTableIndexSQL(tableName string) (sqld, sqlc string) {
	idxs, err := c.GetTableIndexSchema(tableName)
	ec.CheckErr(err)
	for _, i := range idxs {
		idx := "\"" + strings.Replace(strings.Replace(i.Table+`_`+i.Columns+"_idx", "\"", "", -1), ",", "_", -1) + "\""
		exists := ""
		notexists := ""
		switch c.Dest.Driver {
		case "postgres", "pgx":
			exists = "IF EXISTS "
			notexists = "IF NOT EXISTS "
		default:
			exists = ""
			notexists = ""
		}
		sqld += `DROP INDEX ` + exists + `"` + c.DSchema + `".` + idx + `;` + "\n"
		sqlc += `CREATE INDEX ` + notexists + `` + idx + ` ON "` + c.DSchema + `"."` + i.Table + `" (` + i.Columns + `);` + "\n"
	}
	return
}

// GenLink generate table creation
func (c *Conn) GenLink(table string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	tmp := ""
	if table == strings.ToUpper(table) {
		tmp = "TEMP"
	} else {
		tmp = "temp"
	}
	clen := len(cols)
	if c.Dest.Driver == "postgres" || c.Dest.Driver == "pgx" {
		sqld += fmt.Sprintf("\nDROP FOREIGN TABLE IF EXISTS \"%s\".\"%s%s\" CASCADE;\n", c.DSchema, table, tmp)
		sqlc += fmt.Sprintf("CREATE FOREIGN TABLE IF NOT EXISTS \"%s\".\"%s%s\" (\n", c.DSchema, table, tmp)
		for k, c := range cols {
			if k == clen-1 {
				sqlc += fmt.Sprintf("%s\n", c.ColumnName)
			} else {
				sqlc += fmt.Sprintf("%s,\n", c.ColumnName)
			}
		}
		sqlc += ")\n"
		sqlc += fmt.Sprintf("SERVER %s \nOPTIONS (", c.Source.Name)
		sqlc += fmt.Sprintf("table_name '%s.%s', ", c.SSchema, table)
		sqlc += "row_estimate_method 'showplan_all', "
		sqlc += "match_column_names '0');\n"
	} else if c.Dest.Driver == "mssql" {
		sqld += fmt.Sprintf("\nDROP VIEW \"%s\".\"%s%s\";\n", c.DSchema, table, tmp)
		sqlc += fmt.Sprintf("CREATE VIEW \"%s\".\"%s%s\" AS\nSELECT\n", c.DSchema, table, tmp)
		for k, c := range cols {
			collation := ""
			if c.DataType == "CHAR" ||
				c.DataType == "VARCHAR" ||
				c.DataType == "NCHAR" ||
				c.DataType == "NVARCHAR" {
				collation = "COLLATE database_default "
			}
			if k == clen-1 {
				sqlc += fmt.Sprintf("\"%s\" %s\"%s\"\n", c.ColumnName, collation, c.ColumnName)
			} else {
				sqlc += fmt.Sprintf("\"%s\" %s\"%s\",\n", c.ColumnName, collation, c.ColumnName)
			}
		}
		sqlc += fmt.Sprintf("FROM \"%s\".\"%s\".\"%s\".\"%s\";\n", c.Source.Hostname, c.Source.Database, c.SSchema, table)
	}
	return sqld, sqlc
}

// GenUpdate generate update procedure
func (c *Conn) GenUpdate(table string, cols []Column, pkey []PKey) (sqld, sqlc string) {
	columns := trimCols(cols, pkey)

	sqld, sqlc = tableUpdProcStart(c.Dest.Driver, c.DSchema, table)
	sqlc += tableDeleteSQL(c.Dest.Driver, c.DSchema, table, pkey, cols)
	if len(pkey) != len(cols) {
		sqlc += tableUpdateSQL(c.Dest.Driver, c.DSchema, table, pkey, columns)
	}
	sqlc += tableInsertSQL(c.Dest.Driver, c.DSchema, table, pkey, cols)
	sqlc += tableUpdProcEnd(c.Dest.Driver, table)
	return
}

func tableUpdProcStart(destDriver, schema, tableName string) (sqld, sqlc string) {
	tmp := ""
	if tableName == strings.ToUpper(tableName) {
		tmp = "TEMP"
	} else {
		tmp = "temp"
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqld += fmt.Sprintf("\nDROP PROCEDURE IF EXISTS \"%s\".\"upd_%s\"();", schema, tableName)
		sqlc += fmt.Sprintf("\nCREATE OR REPLACE PROCEDURE \"%s\".\"upd_%s\"()\nLANGUAGE plpgsql\nAS $procedure$\nBEGIN\n", schema, tableName)
	} else if destDriver == "mssql" {
		sqld += fmt.Sprintf("\nDROP PROCEDURE \"%s\".\"upd_%s\";", schema, tableName)
		sqlc += fmt.Sprintf("\nCREATE PROCEDURE \"%s\".\"upd_%s\" AS\nBEGIN\n", schema, tableName)
		sqlc += fmt.Sprintf("IF OBJECT_ID('tempdb..#%s','U') IS NOT NULL DROP TABLE tempdb.#%s\n", tableName, tableName)
		sqlc += fmt.Sprintf("SELECT * INTO #%s FROM \"%s\".\"%s%s\"\n", tableName, schema, tableName, tmp)
	}
	return sqld, sqlc
}

func tableUpdProcEnd(destDriver, tableName string) (sqlc string) {
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += "END\n$procedure$;\n"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("IF OBJECT_ID('tempdb..#%s','U') IS NOT NULL DROP TABLE tempdb.#%s\n", tableName, tableName)
		sqlc += "END;\n"
	}
	return sqlc
}

func tableDeleteSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Column) (sqlc string) {
	ttemp := "temp"
	if schema == "ep1" {
		ttemp = "TEMP"
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += "DELETE\n"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("DELETE \"%s\".\"%s\"\n", schema, tableName)
	}
	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += fmt.Sprintf("USING \"%s\".\"%s\" AS d\n", schema, tableName)
		sqlc += fmt.Sprintf("LEFT OUTER JOIN \"%s\".\"%s%s\" \"%s%s\" ON", schema, tableName, ttemp, tableName, ttemp)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("LEFT JOIN #%s \"%s%s\" ON", tableName, ttemp, tableName)
	}
	plen := len(pkey)
	for k, p := range pkey {
		if destDriver == "postgres" || destDriver == "pgx" {
			sqlc += fmt.Sprintf("\nd.\"%s\" = \"%s%s\".\"%s\"", p.PKey, tableName, ttemp, p.PKey)
		} else if destDriver == "mssql" {
			sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%s%s\".\"%s\"", tableName, p.PKey, tableName, ttemp, p.PKey)
		}
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += "WHERE"
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		for k, p := range pkey {
			sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = d.\"%s\" ", tableName, p.PKey, p.PKey)
			if k == plen-1 {
				sqlc += "\n"
			} else {
				sqlc += " AND "
			}
		}
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += fmt.Sprintf("AND \"%s%s\".\"%s\" IS NULL;\n", tableName, ttemp, allColumns[0].ColumnName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("WHERE \"%s%s\".\"%s\" IS NULL\n", tableName, ttemp, allColumns[0].ColumnName)
	}
	return sqlc
}

func tableUpdateSQL(destDriver, schema, tableName string, pkey []PKey, columns []Column) (sqlc string) {
	ttemp := "temp"
	if schema == "ep1" {
		ttemp = "TEMP"
	}

	sqlc += fmt.Sprintf("UPDATE \"%s\".\"%s\"\nSET", schema, tableName)
	plen := len(pkey)
	clen := len(columns)
	for k, c := range columns {
		sqlc += fmt.Sprintf("\n\"%s\" = \"%s%s\".\"%s\"", c.ColumnName, tableName, ttemp, c.ColumnName)
		if k == clen-1 {
			sqlc += ""
		} else {
			sqlc += ","
		}
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += fmt.Sprintf("\nFROM \"%s\".\"%s%s\" \"%s%s\"", schema, tableName, ttemp, tableName, ttemp)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("\nFROM #%s \"%s%s\"", tableName, ttemp, tableName)
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += "\nWHERE"
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("\nJOIN \"%s\".\"%s\" ON", schema, tableName)
	}
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%s%s\".\"%s\"", tableName, p.PKey, tableName, ttemp, p.PKey)
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += "AND ("
	} else if destDriver == "mssql" {
		sqlc += "WHERE ("
	}
	for k, c := range columns {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" <> \"%s%s\".\"%s\"", tableName, c.ColumnName, tableName, ttemp, c.ColumnName)
		if k == clen-1 {
			sqlc += "\n"
		} else {
			sqlc += " OR "
		}
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += ");\n"
	} else if destDriver == "mssql" {
		sqlc += ")\n"
	}
	return sqlc
}

func tableInsertSQL(destDriver, schema, tableName string, pkey []PKey, allColumns []Column) (sqlc string) {
	ttemp := "temp"
	if schema == "ep1" {
		ttemp = "TEMP"
	}

	plen := len(pkey)
	clen := len(allColumns)
	sqlc += fmt.Sprintf("INSERT INTO \"%s\".\"%s\"\n", schema, tableName)
	sqlc += "SELECT"
	for k, c := range allColumns {
		sqlc += fmt.Sprintf("\n\"%s%s\".\"%s\" \"%s\"", tableName, ttemp, c.ColumnName, c.ColumnName)
		if k == clen-1 {
			sqlc += "\n"
		} else {
			sqlc += ","
		}
	}
	sqlc += fmt.Sprintf("FROM \"%s\".\"%s\"\n", schema, tableName)
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += fmt.Sprintf("RIGHT JOIN \"%s\".\"%s%s\" \"%s%s\" ON", schema, tableName, ttemp, tableName, ttemp)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("RIGHT JOIN #%s \"%s%s\" ON", tableName, tableName, ttemp)
	}
	for k, p := range pkey {
		sqlc += fmt.Sprintf("\n\"%s\".\"%s\" = \"%s%s\".\"%s\"", tableName, p.PKey, tableName, ttemp, p.PKey)
		if k == plen-1 {
			sqlc += "\n"
		} else {
			sqlc += " AND "
		}
	}
	if destDriver == "postgres" || destDriver == "pgx" {
		sqlc += fmt.Sprintf("WHERE \"%s\".\"%s\" IS NULL;\n", tableName, allColumns[0].ColumnName)
	} else if destDriver == "mssql" {
		sqlc += fmt.Sprintf("WHERE \"%s\".\"%s\" IS NULL\n", tableName, allColumns[0].ColumnName)
	}
	return sqlc
}
