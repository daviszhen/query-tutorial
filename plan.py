from pglast import ast,enums
from constants import *
from catalog import *
# plan构造器
class LogicalPlanBuilder:
    def __init__(self):
        pass

    def build(self,node,plan : dict):
        """
        构建逻辑plan
        :param node: 节点ast
        :param plan: 逻辑计划
        :return:
        """
        pass


# SELECT语句构造器。目前只支持tpch q1
class SelectBuilder(LogicalPlanBuilder):
    def __init__(self):
        super().__init__()

    def build(self,select : ast.SelectStmt,plan : dict):
        fb = FromBuilder()
        fb.build(select.fromClause,plan)

        wb = WhereBuilder()
        wb.build(select.whereClause,plan)

        selectList = SelectListBuilder()
        selectList.build(select.targetList,plan)

        groupby = GroupbyBuilder()
        groupby.build(select.groupClause,plan)

        orderby = OrderbyBuilder()
        orderby.build(select.sortClause,plan)

        project =  ProjectListBuilder()
        project.build(select.targetList,plan)

        pass

class FromBuilder(LogicalPlanBuilder):
    def __init__(self):
        super().__init__()
    def build(self,tableRefs : tuple,plan : dict):
        if len(tableRefs) == 1:
            single = self.buildTableRef(tableRefs[0])
            plan[RELATIONS] = [SINGLE_RELATION,single]
            return
        raise Exception("unsupport multiple table refs")
        pass

    def buildTableRef(self,tableRef : ast.RangeVar)-> dict:
        dbName = tableRef.schemaname
        if dbName is None or len(dbName) == 0:
            dbName = "tpch"

        # 从catalog中取表定义
        if tableRef.relname in Catalog[dbName] :
            return {tableRef.relname :
                    [RELATION_TYPE_TABLE, #relation type
                    dbName, #database name
                    tableRef.relname,#alias=
                    tableRef.relname,#original name
                    Catalog[dbName][tableRef.relname] #schema
                    ]}
        else:
            raise Exception("no such table in Catalog",tableRef.schemaname,tableRef.relname)
        pass

class ExpressionBuilder(LogicalPlanBuilder):
    def __init__(self):
        super().__init__()
        self.in_agg_func = False

    def set_in_agg_func(self,t):
        self.in_agg_func = t

    def is_in_agg_func(self):
        return self.in_agg_func

    def build(self,node,plan : dict):
        """根据ast的类型。构建表达式。结果是tuple类型"""
        if isinstance(node,ast.A_Expr):
            if node.kind == enums.parsenodes.A_Expr_Kind.AEXPR_OP:
                opName = node.name[0].sval
                if opName == "<=":
                    l = self.build(node.lexpr, plan)
                    r = self.build(node.rexpr, plan)
                    return EXPR_OP_LESS_EQUAL, l, r
                elif opName == "-":
                    l = self.build(node.lexpr, plan)
                    r = self.build(node.rexpr, plan)
                    return EXPR_OP_MINUS, l, r
                elif opName == "*":
                    l = self.build(node.lexpr, plan)
                    r = self.build(node.rexpr, plan)
                    return EXPR_OP_MULTI, l, r
                elif opName == "+":
                    l = self.build(node.lexpr, plan)
                    r = self.build(node.rexpr, plan)
                    return EXPR_OP_PLUS, l, r
                elif opName == ">":
                    l = self.build(node.lexpr, plan)
                    r = self.build(node.rexpr, plan)
                    return EXPR_OP_MORE, l, r
                else:
                    raise Exception("unsupported operator", node)
            else:
                raise Exception("unsupported expr 1", node)
        elif isinstance(node,ast.ColumnRef):
            return self.build_column_ref(node,plan)
        elif isinstance(node,ast.TypeCast):
            e = self.build(node.arg,plan)
            t = node.typeName.names
            return EXPR_OP_CAST,e,t
        elif isinstance(node,ast.A_Const):
            if node.isnull:
                return EXPR_OP_CONST,node.isnull
            return EXPR_OP_CONST,node.isnull,node.val
        elif isinstance(node,ast.FuncCall):
            return self.build_func_call(node,plan)
        elif isinstance(node, ast.ResTarget):
            return self.build_res_target(node,plan)
        else:
            raise Exception("unsupported expr 2",node)
        pass

    def build_res_target(self,node,plan):
        value = node.val
        if isinstance(value, ast.ColumnRef):
            r = self.build(value, plan)
            alias = node.name
            if not isValidName(alias):
                # 取列名
                alias = r[1][0].name
            return self.make_res_expr_res_target(node, plan, alias, r)
        elif isinstance(value, ast.FuncCall):
            r = self.build(value, plan)
            alias = node.name
            if not isValidName(alias):
                # 取表达式字符串
                alias = str(value)
            return self.make_res_expr_res_target(node, plan, alias, r)
        else:
            raise Exception(f"not implement res target {value}")

    def make_res_expr_res_target(self, node, plan, alias, expr):
        raise Exception(f"subclass should implement {alias}, {expr}")

    def build_column_ref(self,node,plan : dict):
        fields = node.fields
        if len(fields) == 1:
            col_name = fields[0].sval
            col_ref = getColumn(plan, "", "", col_name)
            return EXPR_OP_COLUMN_REF, col_ref
        elif len(fields) == 2:
            table_name = fields[0].sval
            col_name = fields[1].sval
            col_ref = getColumn(plan, "", table_name, col_name)
            return EXPR_OP_COLUMN_REF, col_ref
        raise Exception("unsupported column ref", node)

    def build_func_call(self,node, plan : dict):
        func_name = node.funcname[0].sval
        is_agg_func = False
        if is_aggregate_func(func_name):
            is_agg_func = True

        if is_agg_func and self.is_in_agg_func():
            raise  Exception(f"can not use agg func {func_name} inside a agg func")

        if is_agg_func:
            self.set_in_agg_func(True)

        args = None
        if node.args is not None:
            args = []
            for arg in node.args:
                arg_e = self.build(arg, plan)
                args.append(arg_e)
        elif node.agg_star:
            args = "*"
        else:
            raise Exception("function has no args", node)
        if is_agg_func:
            ret_expr = self.build_agg_func(node,plan,func_name,args)
            self.set_in_agg_func(False)
            return ret_expr
        return FUNC_EXPR, func_name, args

    def build_agg_func(self,node,plan : dict,name,args):
        aggs = plan.get(AGGREGATE, [])
        agg_idx = len(aggs)
        aggs.append((FUNC_EXPR, name, args))
        plan[AGGREGATE] = aggs
        return AGG_FUNC_RESULT_REF_EXPR, agg_idx

def is_aggregate_func(name):
    return name in ["count","avg","sum"]

class WhereExprBuilder(ExpressionBuilder):
    def __init__(self):
        super().__init__()

    def build_agg_func(self,node,plan : dict,name,args):
        raise Exception(f"can not use agg func {name} in WHERE")

class WhereBuilder(LogicalPlanBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        eb = WhereExprBuilder()
        ret = eb.build(node,plan)
        plan[WHERE] = ret

class SelectExprBuilder(ExpressionBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        return super().build(node,plan)

    def make_res_expr_res_target(self,node,plan,alias,r):
        return OUTPUT_EXPR,alias,r

class SelectListBuilder(LogicalPlanBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        selExprBuilder = SelectExprBuilder()
        output = []
        for expr in node:
            o = selExprBuilder.build(expr,plan)
            output.append(o)
        plan[OUTPUT] = output

"""
From https://www.postgresql.org/docs/current/sql-select.html
Group By Clause:
 GROUP BY [ ALL | DISTINCT ] grouping_element [, ...]

 An expression used inside a grouping_element can be 
 an input column name, 
 or the name or ordinal number of an output column (SELECT list item), 
 or an arbitrary expression formed from input-column values.
  
 In case of ambiguity, a GROUP BY name will be 
 interpreted as an input-column name rather than 
 an output column name.
 
 这里只考虑group by带input column name的情况。
 
 其它两种表达式，会引入复杂的情形。见下面。
"""

"""

create table t (
a int,
b int,
c int
);

insert into t values(1,1,1);
insert into t values(1,1,1);
insert into t values(1,1,1);
insert into t values(2,2,2);
insert into t values(2,2,2);
insert into t values(3,1,2);

select count(a) from t group by 1;
psql:commands.sql:14: ERROR:  aggregate functions are not allowed in GROUP BY

select count(a),b from t group by 1+b;
psql:commands.sql:14: ERROR:  column "t.b" must appear in the GROUP BY clause or be used in an aggregate function

select count(a),b+1 from t group by 1+b;
psql:commands.sql:14: ERROR:  column "t.b" must appear in the GROUP BY clause or be used in an aggregate function

select count(a),1+b from t group by 1+b;

select count(a),2*(1+b),(1+b)*2 from t group by 1+b;
select count(a),2*(1+b+3),(1+b+3)*2 from t group by 1+b;

select count(a),2*(3+1+b),(3+1+b)*2 from t group by 1+b;
psql:commands.sql:15: ERROR:  column "t.b" must appear in the GROUP BY clause or be used in an aggregate function

select count(a),2+b from t group by 1+b;
psql:commands.sql:14: ERROR:  column "t.b" must appear in the GROUP BY clause or be used in an aggregate function

select count(a),1+b+1 from t group by 1+b;

select count(a),1+1+b from t group by 1+b;
psql:commands.sql:14: ERROR:  column "t.b" must appear in the GROUP BY clause or be used in an aggregate function

select count(a) from t group by a+b;

select count(a),a+b from t group by a+b;

select count(a),2*(1+b),(1+b)*2 from t group by 2*(1+b);

select count(a),2*(1+b),3 * (1+b) from t group by 2*(1+b);
psql:commands.sql:15: ERROR:  column "t.b" must appear in the GROUP BY clause or be used in an aggregate function

select count(a),2*(1+b)+c from t group by 2*(1+b);
psql:commands.sql:15: ERROR:  column "t.c" must appear in the GROUP BY clause or be used in an aggregate function

select count(a),2*(1+b)+count(c) from t group by 2*(1+b);

select count(a),b from t group by 2;

select count(a),b*2,b*3 from t group by 2;
psql:commands.sql:15: ERROR:  column "t.b" must appear in the GROUP BY clause or be used in an aggregate function

"""
class GroupExprBuilder(ExpressionBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        if isinstance(node,ast.ColumnRef):
            r = super().build(node,plan)
            return GROUP_EXPR,r
        else:
            raise Exception(f"only support column ref in GROUP BY")

class GroupbyBuilder(LogicalPlanBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        geb = GroupExprBuilder()
        groupby = []
        for expr in node:
            r = geb.build(expr,plan)
            groupby.append(r)
        plan[GROUP] = groupby
        pass

"""
From: https://www.postgresql.org/docs/current/sql-select.html
Order By Clause:
the name or ordinal number of an output column (SELECT list item), 
or it can be an arbitrary expression formed from input-column values.


"""
class OrderExprBuilder(ExpressionBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        if isinstance(node,ast.SortBy):
            e = node.node
            r = None
            outputs = plan.get(OUTPUT, [])
            if len(outputs) == 0:
                raise Exception("there is no output list")
            #ORDER BY 直接加列名。
            #如果没有表前缀，先在output list中找别名
            #之后从表中找定义
            if isinstance(e,ast.ColumnRef):
                fields = e.fields
                if len(fields) == 1:
                    col_name = fields[0].sval
                    # 先判断，列名是否是output list中的alias
                    found = False
                    for i in range(len(outputs)):
                        alias = outputs[i][1]
                        # 找到output alias，引用表达式值
                        if col_name == alias:
                            r =  OUTPUT_EXPR_RESULT_REF_EXPR, i
                            found = True
                            break

                    if not found:
                        # 不是，就到表中找
                        r = super().build_column_ref(e, plan)

                else:
                    r =  super().build_column_ref(e, plan)
            #ORDER BY 1,2,3的情形
            elif isinstance(e,ast.A_Const):
                if e.isnull:
                    raise Exception(f"need integer but get NULL")
                elif isinstance(e.val,ast.Integer):
                    v = e.val.ival
                    if v < 0:
                        v = -v
                    if v < 1 or v > len(outputs):
                        raise Exception(f"order by index {v} is out of range of the output list {len(outputs)}")
                    r = OUTPUT_EXPR_RESULT_REF_EXPR, v-1
                else:
                    raise Exception(f"need integer but get {e}")
            else:
                r = super().build(e,plan)
            return ORDER_EXPR,r,node.sortby_dir
        else:
            raise Exception(f"not implement order expr {node}")
    def build_agg_func(self,node,plan : dict,name,args):
        raise Exception(f"can not use agg func {name} in ORDER BY")

class OrderbyBuilder(LogicalPlanBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        oeb = OrderExprBuilder()
        orderby = []
        for expr in node:
            r = oeb.build(expr,plan)
            orderby.append(r)
        plan[ORDER] = orderby
        pass

class ProjectExprBuilder(ExpressionBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        return super().build(node,plan)

    def make_res_expr_res_target(self,node,plan,alias,r):
        return PROJECT_EXPR,alias,r
class ProjectListBuilder(LogicalPlanBuilder):
    def __init__(self):
        super().__init__()

    def build(self,node,plan : dict):
        projectExprBuilder = ProjectExprBuilder()
        projects = []
        for expr in node:
            p = projectExprBuilder.build(expr,plan)
            projects.append(p)
        plan[PROJECT] = projects
