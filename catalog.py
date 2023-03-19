import pyarrow as arrow
from constants import *

lineitem_path = "../mo-test/tpch100M/lineitem.tbl"
lineitem_delimiter = "|"

lineitemSchema = arrow.schema([
    arrow.field("l_orderkey",arrow.int64(),False),
    arrow.field("l_partkey",arrow.int32(),False),
    arrow.field("l_suppkey",arrow.int32(),False),
    arrow.field("l_linenumber",arrow.int32(),False),
    arrow.field("l_quantity",arrow.float64(),False),
    arrow.field("l_extendedprice",arrow.float64(),False),
    arrow.field("l_discount",arrow.float64(),False),
    arrow.field("l_tax",arrow.float64(),False),
    arrow.field("l_returnflag",arrow.utf8(),False),
    arrow.field("l_linestatus",arrow.utf8(),False),
    arrow.field("l_shipdate",arrow.date32(),False),
    arrow.field("l_commitdate",arrow.date32(),False),
    arrow.field("l_receiptdate",arrow.date32(),False),
    arrow.field("l_shipinstruct",arrow.utf8(),False),
    arrow.field("l_shipmode",arrow.utf8(),False),
    arrow.field("l_comment",arrow.utf8(),False),
],{"name":"lineitem",
   "delimiter":lineitem_delimiter,
   "path":lineitem_path})

Catalog = {
    "tpch":{
        "lineitem":lineitemSchema
    }
}

# Catalog

def isValidName(s):
    return not (s is None or len(s) == 0)


def getColumn(plan,dbName,tableName,colName):
    """取列定义"""
    relations = plan.get(RELATIONS,None)
    if relations is None:
        raise Exception("no table defs")

    if len(dbName) == 0:
        dbName = "tpch"

    if relations[0] == SINGLE_RELATION:
        single = relations[1]
        if len(tableName) == 0:
            #在每个表中找字段
            for tableName2,tableDef in single.items():
                schema = tableDef[SCHEMA_IDX]
                colDef = schema.field(colName)
                if colDef is not None:
                    return colDef,dbName,tableName2
            raise Exception(f"no column name {colName} in table {tableName}")

        #找到表定义
        if tableName in single:
            tableDef = single[tableName]
            if tableDef[DB_NAME_IDX] == dbName and tableDef[ALIAS_IDX] == tableName:
                schema = tableDef[SCHEMA_IDX]
                colDef = schema.field(colName)
                if colDef is not None:
                    return colDef,dbName,tableName
                else:
                    raise Exception(f"no column name {colName} in table {tableName}")
            else:
                raise Exception(f"invalid database name {dbName} or table name {tableName}")
        else:
            raise Exception(f"no such relation {tableName} in database {dbName}")
    else:
        raise Exception("not implement multiple relations")

