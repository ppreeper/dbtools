package database

import (
	"context"
	"fmt"
	"time"
)

//########
// Indexes
//########

// Index list of Indexes
type Index struct {
	Schema     string `db:"schemaname"`
	Table      string `db:"tablename"`
	Name       string `db:"indexname"`
	Columns    string `db:"indexcolumns"`
	Definition string `db:"indexdef"`
}

type IndexList struct {
	Name string `db:"indexname"`
}

// GetIndexes returns list of Indexes and definitions
func (c *Conn) GetIndexes(schema string, timeout int) ([]IndexList, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
	defer cancel()
	q := ""
	switch c.Source.Driver {
	case "postgres", "pgx":
		q += `select p.indexname from pg_catalog.pg_indexes p
		left join (
			SELECT CONSTRAINT_NAME`
		q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS", c.Source.Database)
		q += `where CONSTRAINT_TYPE = 'PRIMARY KEY'
		) c on p.indexname = c.constraint_name
		where p.schemaname not in ('information_schema','pg_catalog')
		and c.constraint_name is null
		and p.schemaname = $1`
	case "mssql":
		q += `select i."name" "indexname" from sys.objects t
		inner join sys.indexes i on t.object_id = i.object_id
		cross apply (
			select col."name" + ', '
			from sys.index_columns ic
			inner join sys.columns col
			on ic.object_id = col.object_id
			and ic.column_id = col.column_id
			where ic.object_id = t.object_id
			and ic.index_id = i.index_id
			order by col.column_id for xml path ('')
		) D (column_names)
		where t.is_ms_shipped <> 1 AND t."type" = 'U'
		and index_id > 0 and i.is_primary_key <> 1
		and schema_name(t.schema_id) = ?
		order by schema_name(t.schema_id) + '.' + t."name", i.index_id`
	}
	vv := []IndexList{}
	if err := c.Source.SelectContext(ctx, &vv, q, schema); err != nil {
		return nil, fmt.Errorf("select: %w", err)
	}
	return vv, nil
}

// GetIndexeschema returns Indexes and definition
func (c *Conn) GetIndexSchema(schema, index string) (Index, error) {
	q := ""
	switch c.Source.Driver {
	case "postgres", "pgx":
		q += `select p.schemaname,p.tablename,p.indexname
		,'"'||replace(replace(split_part(split_part(p.indexdef,'(',2),')',1),'"',''),',','","')||'"' as indexcolumns
		,p.indexdef
		from pg_catalog.pg_indexes p
		left join (
			SELECT CONSTRAINT_NAME`
		q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS", c.Source.Database)
		q += `) c on p.indexname = c.constraint_name
		where p.schemaname not in ('information_schema','pg_catalog')
		and c.constraint_name is null
		and p.schemaname = $1 and p.indexname = $2
		order by schemaname,tablename`
	case "mysql":
		q += `select schema_name(t.schema_id) "schemaname"
		,t."name" "tablename"
		,i."name" "indexname"
		,'"'+replace(substring(column_names, 1, len(column_names)-1),', ','","')+'"' as indexcolumns
		,'CREATE '+
		case when i."type" = 1 then 'CLUSTERED'
		when i."type" = 2 then 'NONCLUSTERED UNIQUE'
		when i."type" = 3 then 'XML'
		when i."type" = 4 then 'SPATIAL'
		when i."type" = 5 then 'CLUSTERED COLUMNSTORE'
		when i."type" = 6 then 'NONCLUSTERED COLUMNSTORE'
		when i."type" = 7 then 'NONCLUSTERED HASH'
		end + ' INDEX ' +i."name" + ' ON '
		+ schema_name(t.schema_id) + '.' + t."name"
		+ '('+substring(column_names, 1, len(column_names)-1)+');'
		AS "indexdef"
		from sys.objects t
		inner join sys.indexes i on t.object_id = i.object_id
		cross apply (
			select col."name" + ', '
			from sys.index_columns ic
			inner join sys.columns col
			on ic.object_id = col.object_id
			and ic.column_id = col.column_id
			where ic.object_id = t.object_id
			and ic.index_id = i.index_id
			order by col.column_id for xml path ('')
			) D (column_names)
		where t.is_ms_shipped <> 1 AND t."type" = 'U'
		and index_id > 0 and i.is_primary_key <> 1
		and schema_name(t.schema_id) = ?
		and i."name" = ?
		order by schema_name(t.schema_id) + '.' + t."name", i.index_id`
	}
	vv := Index{}
	if err := c.Source.Get(&vv, q, schema, index); err != nil {
		return Index{}, fmt.Errorf("select: %w", err)
	}
	return vv, nil
}

// GetIndexeschema returns Indexes and definition
func (c *Conn) GetTableIndexSchema(table string) ([]Index, error) {
	q := ""
	switch c.Source.Driver {
	case "postgres", "pgx":
		q += "select p.schemaname,p.tablename,p.indexname" + "\n"
		q += `,'"'||replace(replace(split_part(split_part(p.indexdef,'(',2),')',1),'"',''),',','","')||'"' as indexcolumns` + "\n"
		q += `,p.indexdef` + "\n"
		q += `from pg_catalog.pg_indexes p` + "\n"
		q += `left join (` + "\n"
		q += `SELECT CONSTRAINT_NAME` + "\n"
		q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS", c.Dest.Database) + "\n"
		q += `) c on p.indexname = c.constraint_name` + "\n"
		q += `where p.schemaname not in ('information_schema','pg_catalog')` + "\n"
		q += `and c.constraint_name is null` + "\n"
		q += `and p.schemaname = $1 and p.tablename = $2` + "\n"
		q += `order by schemaname,tablename,indexname`
	case "mssql":
		q += `select schema_name(t.schema_id) "schemaname"` + "\n"
		q += `,t."name" "tablename"` + "\n"
		q += `,i."name" "indexname"` + "\n"
		q += `,'"'+replace(substring(column_names, 1, len(column_names)-1),', ','","')+'"' as indexcolumns` + "\n"
		q += `,'CREATE '+` + "\n"
		q += `case when i."type" = 1 then 'CLUSTERED'` + "\n"
		q += `when i."type" = 2 then 'NONCLUSTERED UNIQUE'` + "\n"
		q += `when i."type" = 3 then 'XML'` + "\n"
		q += `when i."type" = 4 then 'SPATIAL'` + "\n"
		q += `when i."type" = 5 then 'CLUSTERED COLUMNSTORE'` + "\n"
		q += `when i."type" = 6 then 'NONCLUSTERED COLUMNSTORE'` + "\n"
		q += `when i."type" = 7 then 'NONCLUSTERED HASH'` + "\n"
		q += `end + ' INDEX ' +i."name" + ' ON '` + "\n"
		q += `+ schema_name(t.schema_id) + '.' + t."name"` + "\n"
		q += `+ '('+substring(column_names, 1, len(column_names)-1)+');'` + "\n"
		q += `AS "indexdef"` + "\n"
		q += `from sys.objects t` + "\n"
		q += `inner join sys.indexes i on t.object_id = i.object_id` + "\n"
		q += `cross apply (` + "\n"
		q += `select col."name" + ', '` + "\n"
		q += `from sys.index_columns ic` + "\n"
		q += `inner join sys.columns col` + "\n"
		q += `on ic.object_id = col.object_id` + "\n"
		q += `and ic.column_id = col.column_id` + "\n"
		q += `where ic.object_id = t.object_id` + "\n"
		q += `and ic.index_id = i.index_id ` + "\n"
		q += `order by col.column_id for xml path ('') ` + "\n"
		q += `) D (column_names)` + "\n"
		q += `where t.is_ms_shipped <> 1 AND t."type" = 'U'` + "\n"
		q += `and index_id > 0 and i.is_primary_key <> 1` + "\n"
		q += `and schema_name(t.schema_id) = ? and t."name" = ?` + "\n"
		q += `order by schema_name(t.schema_id) + '.' + t."name", i.index_id`
	}
	vv := []Index{}
	if err := c.Source.Select(&vv, q, c.SSchema, table); err != nil {
		return []Index{}, fmt.Errorf("select: %w", err)
	}
	return vv, nil
}

func (db *Database) GetTableIndexSchema(schema, table string) ([]Index, error) {
	q := ""
	if db.Driver == "postgres" || db.Driver == "pgx" {
		q += "select p.schemaname,p.tablename,p.indexname" + "\n"
		q += `,'"'||replace(replace(split_part(split_part(p.indexdef,'(',2),')',1),'"',''),',','","')||'"' as indexcolumns` + "\n"
		q += `,p.indexdef` + "\n"
		q += `from pg_catalog.pg_indexes p` + "\n"
		q += `left join (` + "\n"
		q += `SELECT CONSTRAINT_NAME` + "\n"
		q += fmt.Sprintf("FROM %s.INFORMATION_SCHEMA.TABLE_CONSTRAINTS", db.Name) + "\n"
		q += `) c on p.indexname = c.constraint_name` + "\n"
		q += `where p.schemaname not in ('information_schema','pg_catalog')` + "\n"
		q += `and c.constraint_name is null` + "\n"
		q += `and p.schemaname = '` + schema + `'` + "\n"
		q += `and p.tablename = '` + table + `'` + "\n"
		q += `order by schemaname,tablename,indexname`
	} else if db.Driver == "mssql" {
		q += `select schema_name(t.schema_id) "schemaname"` + "\n"
		q += `,t."name" "tablename"` + "\n"
		q += `,i."name" "indexname"` + "\n"
		q += `,'"'+replace(substring(column_names, 1, len(column_names)-1),', ','","')+'"' as indexcolumns` + "\n"
		q += `,'CREATE '+` + "\n"
		q += `case when i."type" = 1 then 'CLUSTERED'` + "\n"
		q += `when i."type" = 2 then 'NONCLUSTERED UNIQUE'` + "\n"
		q += `when i."type" = 3 then 'XML'` + "\n"
		q += `when i."type" = 4 then 'SPATIAL'` + "\n"
		q += `when i."type" = 5 then 'CLUSTERED COLUMNSTORE'` + "\n"
		q += `when i."type" = 6 then 'NONCLUSTERED COLUMNSTORE'` + "\n"
		q += `when i."type" = 7 then 'NONCLUSTERED HASH'` + "\n"
		q += `end + ' INDEX ' +i."name" + ' ON '` + "\n"
		q += `+ schema_name(t.schema_id) + '.' + t."name"` + "\n"
		q += `+ '('+substring(column_names, 1, len(column_names)-1)+');'` + "\n"
		q += `AS "indexdef"` + "\n"
		q += `from sys.objects t` + "\n"
		q += `inner join sys.indexes i on t.object_id = i.object_id` + "\n"
		q += `cross apply (` + "\n"
		q += `select col."name" + ', '` + "\n"
		q += `from sys.index_columns ic` + "\n"
		q += `inner join sys.columns col` + "\n"
		q += `on ic.object_id = col.object_id` + "\n"
		q += `and ic.column_id = col.column_id` + "\n"
		q += `where ic.object_id = t.object_id` + "\n"
		q += `and ic.index_id = i.index_id ` + "\n"
		q += `order by col.column_id for xml path ('') ` + "\n"
		q += `) D (column_names)` + "\n"
		q += `where t.is_ms_shipped <> 1 AND t."type" = 'U'` + "\n"
		q += `and index_id > 0 and i.is_primary_key <> 1` + "\n"
		q += `and schema_name(t.schema_id) = '` + schema + `'` + "\n"
		q += `and t."name" = '` + table + `'` + "\n"
		q += `order by schema_name(t.schema_id) + '.' + t."name", i.index_id`
	}
	// fmt.Println(q)
	vv := []Index{}
	if err := db.Select(&vv, q); err != nil {
		return []Index{}, fmt.Errorf("select: %w", err)
	}
	return vv, nil
}
