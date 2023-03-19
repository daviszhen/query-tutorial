from unittest import TestCase
from plan import WhereExprBuilder, FromBuilder, SelectExprBuilder, GroupExprBuilder, ExpressionBuilder, \
    OrderExprBuilder, SelectListBuilder
from pglast import parser
from pprint import pprint

def set_up_tables(select, plan):
    fb = FromBuilder()
    fb.build(select.fromClause, plan)


class TestWhereExprBuilder(TestCase):
    def test_build_func_call(self):
        # WHRE中不能用聚合函数
        t1 = parser.parse_sql(
            "select count(lineitem.l_partkey) as x from lineitem where lineitem.l_orderkey > count(lineitem.l_partkey);")[
            0]
        t1plan = {}
        set_up_tables(t1.stmt, t1plan)
        web = WhereExprBuilder()
        self.assertRaises(Exception, web.build, t1.stmt.whereClause, t1plan)

        # select 中可以用聚合函数
        seb = SelectExprBuilder()
        seb.build(t1.stmt.targetList[0], t1plan)


class TestGroupExprBuilder(TestCase):
    def test_build(self):
        # GROUP BY中不能用聚合函数
        t1 = parser.parse_sql(
            "select lineitem.l_partkey from lineitem group by count(lineitem.l_partkey);")[0]
        t1plan = {}
        set_up_tables(t1.stmt, t1plan)
        geb = GroupExprBuilder()
        self.assertRaises(Exception, geb.build, t1.stmt.groupClause[0], t1plan)

        # GROUP BY 1,2,3
        t2 = parser.parse_sql(
            "select lineitem.l_partkey from lineitem group by 1;")[0]
        t2plan = {}
        set_up_tables(t2.stmt, t2plan)
        geb = GroupExprBuilder()
        self.assertRaises(Exception, geb.build, t2.stmt.groupClause[0], t2plan)

        # GROUP BY中只能用关系的列名
        t3 = parser.parse_sql(
            "select lineitem.l_partkey from lineitem group by l_partkey;")[0]
        t3plan = {}
        set_up_tables(t3.stmt, t3plan)
        geb = GroupExprBuilder()
        geb.build(t3.stmt.groupClause[0], t3plan)


class TestExpressionBuilder(TestCase):
    def test_build(self):
        # 聚合函数中不能再嵌套聚合函数
        t1 = parser.parse_sql(
            "select count(sum(lineitem.l_partkey)) from lineitem group by count(lineitem.l_partkey);")[0]
        t1plan = {}
        set_up_tables(t1.stmt, t1plan)
        eb = ExpressionBuilder()
        self.assertRaises(Exception, eb.build, t1.stmt.targetList[0], t1plan)


class TestOrderExprBuilder(TestCase):
    def test1(self):
        # order 正确情况。引用表字段
        t1 = parser.parse_sql(
            "select sum(lineitem.l_partkey) from lineitem order by lineitem.l_partkey;")[0]
        t1plan = {}
        set_up_tables(t1.stmt, t1plan)

        # 构建outputlist
        selectList = SelectListBuilder()
        selectList.build(t1.stmt.targetList, t1plan)

        oeb = OrderExprBuilder()
        res = oeb.build(t1.stmt.sortClause[0], t1plan)

    def test2(self):
        # order 正确情况。引用表字段
        t1 = parser.parse_sql(
            "select sum(lineitem.l_partkey) from lineitem order by l_partkey;")[0]
        t1plan = {}
        set_up_tables(t1.stmt, t1plan)

        # 构建outputlist
        selectList = SelectListBuilder()
        selectList.build(t1.stmt.targetList, t1plan)

        oeb = OrderExprBuilder()
        oeb.build(t1.stmt.sortClause[0], t1plan)

    def test3(self):
        # order 正确情况。引用output list
        t1 = parser.parse_sql(
            "select sum(lineitem.l_partkey) as x from lineitem order by x;")[0]
        t1plan = {}
        set_up_tables(t1.stmt, t1plan)

        # 构建outputlist
        selectList = SelectListBuilder()
        selectList.build(t1.stmt.targetList, t1plan)

        oeb = OrderExprBuilder()
        oeb.build(t1.stmt.sortClause[0], t1plan)

    def test4(self):
        # order 正确情况。引用output list的index
        t1 = parser.parse_sql(
            "select sum(lineitem.l_partkey) as x from lineitem order by 1;")[0]
        t1plan = {}
        set_up_tables(t1.stmt, t1plan)

        # 构建outputlist
        selectList = SelectListBuilder()
        selectList.build(t1.stmt.targetList, t1plan)

        oeb = OrderExprBuilder()
        oeb.build(t1.stmt.sortClause[0], t1plan)

    def test_build(self):
        self.test1()
        self.test2()
        self.test3()
        self.test4()


class Node:
    def __init__(self,t,childs):
        self.children = childs
        self.type =t
    def __str__(self):
        return f"type {self.type} children {self.children}"

class TestTree(TestCase):

    def test(self):
        n1 = Node(1,[0,2])
        pprint(str(n1))