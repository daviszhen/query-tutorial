# logical plan 标签
RELATIONS = "relations"
SINGLE_RELATION = "singleRelation"
MULTI_RELATION = "multipleRelation"
WHERE = "where"
OUTPUT = "output"
GROUP = "group"
ORDER = "order"
AGGREGATE = "aggregate"
PROJECT = "project"

# table def 字段索引
RELATION_TYPE_IDX = 0
DB_NAME_IDX = 1
ALIAS_IDX = 2
TABLE_NAME_IDX = 3
SCHEMA_IDX = 4

# 关系类型
RELATION_TYPE_TABLE = "table"
RELATION_TYPE_VIEW = "view"
RELATION_TYPE_SUBQUERY = "subquery"

# 表达式类型
EXPR_OP_COLUMN_REF = "colRef"
EXPR_OP_LESS_EQUAL = "<="
EXPR_OP_MORE = ">"
EXPR_OP_PLUS = "+"
EXPR_OP_MINUS = "-"
EXPR_OP_MULTI = "*"
EXPR_OP_CAST = "cast"
EXPR_OP_CONST = "const"

FUNC_EXPR = "func"
#引用聚合函数的结果
AGG_FUNC_RESULT_REF_EXPR = "agg_func_result_ref"
#输出表达式结果的引用
OUTPUT_EXPR_RESULT_REF_EXPR = "output_expr_result_ref"
OUTPUT_EXPR = "output_expr"
GROUP_EXPR = "group_expr"
ORDER_EXPR = "order_expr"
PROJECT_EXPR = "project_expr"