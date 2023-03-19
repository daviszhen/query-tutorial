from pglast import ast,parser,visitors,printers,enums
from pprint import pprint
import pyarrow as arrow
from pyarrow import csv,compute,types
import pandas
from datetime import datetime,timedelta,date
import hashlib
from constants import *
from plan import *

q1Stmt = parser.parse_sql(
    "select \
        l_returnflag, \
        l_linestatus, \
        sum(l_quantity) as sum_qty, \
        sum(l_extendedprice) as sum_base_price, \
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price, \
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge, \
        avg(l_quantity) as avg_qty, \
        avg(l_extendedprice) as avg_price, \
        avg(l_discount) as avg_disc, \
        count(*) as count_order \
    from \
        lineitem \
    where \
        l_shipdate <= date '1998-12-01' - interval '112' day \
    group by \
        l_returnflag, \
        l_linestatus \
    order by \
        l_returnflag, \
        l_linestatus;"
)[0]


class PhysicalPlanBuilder:
    def __init__(self):
        pass

    def build(self,plan : dict,node : str):
        """根据当前的节点类型。构建物理执行器。返回Executor实例。"""
        if node == RELATIONS:
            rel_info = plan.get(RELATIONS)
            if rel_info[0] == SINGLE_RELATION:
                rel_def = rel_info[1]
                table_name = list(rel_def.keys())[0]
                table_def = rel_def.get(table_name)
                schema = table_def[SCHEMA_IDX]
                return CsvTableScan(schema,None,[16])
            else:
                raise Exception(f"not implement relations {rel_info[0]}")

        elif node == WHERE :
            child = self.build(plan,RELATIONS)
            # 生成filter执行器
            filter = plan.get(WHERE)
            filter_exec = FilterExecutor(filter,child)
            return filter_exec
        elif node == GROUP:
            child = self.build(plan,WHERE)
            # 生成groupby执行器
            groupby = GroupbyExecutor(plan,child)
            return groupby
        elif node == ORDER:
            child = self.build(plan,GROUP)
            orderby = OrderbyExecutor(plan,child)
            return orderby
        elif node == PROJECT:
            child = self.build(plan,ORDER)
            project = ProjectListExecutor(plan,child)
            return project
        else:
            raise Exception(f"not implement plan")

class Executor:
    def __init__(self):
        pass

    def Open(self):
        pass

    def Next(self):
        pass

    def Close(self):
        pass


class CsvTableScan(Executor):
    def __init__(self,schema : arrow.Schema,column_names : list,drop_columns : list):
        super().__init__()
        self.reader = None
        self.schema = schema
        self.column_names = column_names
        #要删除列的索引
        self.drop_columns = drop_columns
        self.block_size = 16 * 1024

    def Open(self):
        # 打开文件
        meta = self.schema.metadata
        path = meta.get(b"path").decode('utf-8')
        delimiter = meta.get(b"delimiter").decode('utf-8')
        read_opts = arrow.csv.ReadOptions(column_names = self.column_names,
                                          block_size = self.block_size,
                                          autogenerate_column_names = bool)
        parse_opts = arrow.csv.ParseOptions(delimiter = delimiter)
        convert_opts = arrow.csv.ConvertOptions(column_types = self.schema,
                                                include_columns = self.column_names)
        self.reader = arrow.csv.open_csv(path,read_options = read_opts,parse_options = parse_opts,convert_options = convert_opts)

    def Next(self):
        try:
            chunk = self.reader.read_next_batch()
            needed_arrays = []
            for col_idx in range(chunk.num_columns):
                if col_idx in self.drop_columns:
                    continue
                needed_arrays.append(chunk.column(col_idx))
            ret_chunk = arrow.RecordBatch.from_arrays(needed_arrays,self.column_names,self.schema)
            return ret_chunk
        except StopIteration:
            return None

    def Close(self):
        self.reader.close()
        self.reader = None
        self.schema = None
        self.column_names = None
        self.drop_columns = None


def exec_expr(expr,records,agg_offset):
    """
    在输入数据上执行表达式。用pyarrow.compute完成执行。
    :param expr: 表达式
    :param records: 输入数据。列存。
    :param agg_offset: 聚合函数的偏移。用在group算子。
    :return: 执行结果集。
    """
    expr_type = expr[0]
    if expr_type == EXPR_OP_COLUMN_REF:
        return records.column(expr[1][0].name)
    elif expr_type == EXPR_OP_CONST:
        if expr[1]:# NULL
            raise Exception(f"not implement const NULL")
        else:
            if isinstance(expr[2],ast.String):
                return arrow.array([expr[2].sval]*records.num_rows,arrow.string())
            elif isinstance(expr[2],ast.Integer):
                return arrow.array([expr[2].ival]*records.num_rows,arrow.int32())
            else:
                raise Exception(f"not implement const expr {expr}")
    elif expr_type == EXPR_OP_CAST:
        #直接转换
        l = exec_expr(expr[1],records,agg_offset)
        target_type = expr[2][0].sval
        if len(expr[2]) > 1:
            target_type = expr[2][1].sval
        if target_type == "date":
            date_vals = []
            for s in l:
                date_vals.append(datetime.strptime(str(s),"%Y-%m-%d"))
            return arrow.array(date_vals,arrow.date32())
        elif target_type == "interval":
            int_vals = []
            for s in l:
                int_vals.append(int(str(s)))
            return arrow.array(int_vals,arrow.int32())
        else:
            print(f"target_type {target_type}")
            return compute.cast(l,target_type)
    elif expr_type == EXPR_OP_PLUS:
        l = exec_expr(expr[1],records,agg_offset)
        r = exec_expr(expr[2],records,agg_offset)
        return compute.add(l,r)
    elif expr_type == EXPR_OP_MINUS:
        l = exec_expr(expr[1],records,agg_offset)
        r = exec_expr(expr[2],records,agg_offset)

        if types.is_date32(l.type):
            #对时间的减法做特殊处理
            date_time_vals = []
            for d in l:
                date_time_vals.append(datetime.strptime(str(d),"%Y-%m-%d"))
            time_delta_vals = []
            if types.is_int32(r.type):
                for i in r:
                    time_delta_vals.append(timedelta(int(str(i))))
            else:
                raise Exception("date minus needs int32")

            res_vals = [date_time_vals[i] - time_delta_vals[i] for i in range(len(time_delta_vals))]
            return arrow.array(res_vals,arrow.date32())
        else:
            return compute.subtract(l,r)
    elif expr_type == EXPR_OP_MULTI:
        l = exec_expr(expr[1],records,agg_offset)
        r = exec_expr(expr[2],records,agg_offset)
        return compute.multiply(l,r)
    elif expr_type == EXPR_OP_LESS_EQUAL:
        l = exec_expr(expr[1],records,agg_offset)
        r = exec_expr(expr[2],records,agg_offset)
        return compute.less_equal(l,r)
    elif expr_type == GROUP_EXPR:
        return exec_expr(expr[1],records,agg_offset)
    elif expr_type == OUTPUT_EXPR:
        return exec_expr(expr[2],records,agg_offset)
    elif expr_type == AGG_FUNC_RESULT_REF_EXPR:
        return records.column(expr[1]+agg_offset)
    elif expr_type == PROJECT_EXPR:
        return records.column(expr[1])
    else:
        raise Exception(f"not implement exec expr {expr}")
    pass

class FilterExecutor(Executor):
    def __init__(self, filter: tuple, child: Executor):
        super().__init__()
        self.filter = filter
        self.child = child

    def Open(self):
        self.child.Open()

    def Next(self):
        child_records = self.child.Next()
        if child_records is None:
            return None
        mask = exec_expr(self.filter,child_records,0)
        return child_records.filter(mask)

    def Close(self):
        self.child.Close()
        self.filter = None
        self.child = None

class AggFunc:
    def __init__(self):
        pass

    def name(self):
        pass

    def add(self,records,row_idx):
        pass

    def addRecords(self,records):
        pass

    def merge(self,other):
        pass

    def get(self):
        pass

class AggFuncFactory:
    def __init__(self):
        pass

    def create(self,name):
        if name == "sum":
            return AggFuncSum()
        elif name == "count":
            return AggFuncCount()
        elif name == "avg":
            return AggFuncAvg()
        else:
            raise Exception(f"not implement agg func {name}")

class AggFuncSum(AggFunc):
    def __init__(self):
        super().__init__()
        self.sum = None

    def name(self):
        return "sum"

    def add(self,records,row_idx):
        param_val = records[0][row_idx]
        if self.sum is None:
            self.sum = param_val
        else:
            self.sum = compute.add(self.sum,param_val)

    def addRecords(self,records):
        pass

    def merge(self,other):
        pass

    def get(self):
        return self.sum

class AggFuncCount(AggFunc):
    def __init__(self):
        super().__init__()
        self.count = 0

    def name(self):
        return "count"

    def add(self,records,row_idx):
        self.count = self.count + 1

    def addRecords(self,records):
        pass

    def merge(self,other):
        pass

    def get(self):
        return self.count

class AggFuncAvg(AggFunc):
    def __init__(self):
        super().__init__()
        self.count = 0
        self.sum = None

    def name(self):
        return "avg"

    def add(self,records,row_idx):
        param_val = records[0][row_idx]
        if self.sum is None:
            self.sum = param_val
        else:
            self.sum = compute.add(self.sum,param_val)
        self.count = self.count + 1

    def addRecords(self,records):
        pass

    def merge(self,other):
        pass

    def get(self):
        return compute.divide(self.sum , self.count)

def get_field_from_groupby(e):
    """从groupby表达式中取出pyarrow.Field"""
    typ = e[1][0]
    if typ == EXPR_OP_COLUMN_REF:
        return e[1][1][0]
    else:
        raise Exception(f"not implement group by {e}")

def get_field_from_value(s):
    """将值类型转成pyarrow类型"""
    if isinstance(s,arrow.StringScalar):
        return s.type
    elif isinstance(s,arrow.Decimal256Scalar):
        return s.type
    elif isinstance(s,arrow.Int64Scalar):
        return s.type
    elif isinstance(s,arrow.DoubleScalar):
        return s.type
    elif isinstance(s,int):
        return arrow.int64()
    elif isinstance(s,str):
        return arrow.string()
    else:
        raise Exception(f"not implement scalar type {s} {type(s)}")

def convert_col_to_array(col,typ):
    """将输入列转成pyarrow.Array"""
    if isinstance(col[0],arrow.StringScalar):
        return arrow.array([ss.as_py() for ss in col],typ)
    elif isinstance(col[0],arrow.Decimal256Scalar):
        return arrow.array([ss.as_py() for ss in col],typ)
    elif isinstance(col[0],arrow.DoubleScalar):
        return arrow.array([ss.as_py() for ss in col],typ)
    elif isinstance(col[0],int):
        return arrow.array(col,typ)
    elif isinstance(col[0],str):
        return arrow.array(col,typ)
    else:
        raise Exception(f"not implement col type {col}")

class GroupbyExecutor(Executor):
    def __init__(self, plan: dict, child: Executor):
        super().__init__()
        self.plan = plan
        self.child = child
        self.aggregate = plan.get(AGGREGATE,[])
        self.groupby = plan.get(GROUP,[])
        self.output = plan.get(OUTPUT,[])
        self.hash_func = hashlib.sha256()
        #构建哈希表
        self.hash_table = {}
        self.agg_factory = AggFuncFactory()

    def update_agg_func_val(self,hash_key,group_by_vals,param_vals,row_idx):
        """
        更新聚合函数的中间结果。
        中间结果的分布：
            groupby1_val, groupby2_val, ..., agg1_val, agg2_val, ...,
        :param hash_key:
        :param group_by_vals:group by表达式值
        :param param_vals:参数表达式值
        :param row_idx:行号。这行值要更新到聚合结果中。
        :return:
        """
        #取中间结果
        hash_val = self.hash_table.get(hash_key,[])
        if len(hash_val) == 0:
            agg_func_vals = []
            for agg_func_expr in self.aggregate:
                agg_func_name = agg_func_expr[1]
                intermediate_result = self.agg_factory.create(agg_func_name)
                agg_func_vals.append(intermediate_result)
            group_by_row_vals = []
            for v in group_by_vals:
                group_by_row_vals.append(v[row_idx])
            # groupby1, groupby2, ..., agg1,agg2, ...,
            hash_val = [group_by_row_vals,agg_func_vals]
            self.hash_table[hash_key] = hash_val

        #更新中间结果
        for agg_idx in range(len(self.aggregate)):
            agg_func_val = hash_val[1][agg_idx]
            param_val = param_vals[agg_idx]
            agg_func_val.add(param_val,row_idx)


    def end_agg_func_val(self):
        #取聚合的最终结果
        # groupby1, groupby2, ..., agg1,agg2,...,
        result_cols = []
        for hash_key,hash_val in self.hash_table.items():
            if len(result_cols) == 0:
                result_cols = [[] for _ in range(len(hash_val[0]) + len(hash_val[1]))]
            #拼接group_by
            for i in range(len(hash_val[0])):
                result_cols[i].append(hash_val[0][i])

            ##拼接聚合函数结果
            begin = len(hash_val[0])
            for i in range(len(hash_val[1])):
                j = begin + i
                result_cols[j].append(hash_val[1][i].get())

        # 定义schema，此处直接拿第一行的结果类型。
        # 正确的做法应该是类型推断
        result_types = []
        for i in range(len(result_cols)):
            col = result_cols[i]
            v = col[0]
            if i < len(self.groupby):
                result_types.append(get_field_from_groupby(self.groupby[i]))
            else:
                t = get_field_from_value(v)
                result_types.append((str(i - len(self.groupby)),t))

        schema = arrow.schema(result_types)

        #确定数据
        result_arr = []
        for i in range(len(result_cols)):
            col = result_cols[i]
            result_arr.append(convert_col_to_array(col,schema.field(i).type))
        return arrow.record_batch(result_arr,schema)

    def update_hash_table(self,group_by_vals,hash_keys,records):
        """
        更新每个组（hash key区分）的聚合函数值。
        :param group_by_vals: groupby表达式的值
        :param hash_keys: groupby表达式的值的hash值
        :param records:
        :return:
        """
        # 计算聚合函数的参数表达式
        all_arg_vals = []
        for agg_idx in range(len(self.aggregate)):
            agg_func = self.aggregate[agg_idx]
            # 计算每个参数的值
            agg_func_name = agg_func[1]
            agg_args = agg_func[2]
            agg_arg_vals = []
            for arg in agg_args:
                if agg_func_name == "count" and arg == "*":
                    agg_arg_vals.append(arrow.array(["*"]*records.num_rows,arrow.string()))
                else:
                    agg_arg_val = exec_expr(arg,records,0)
                    agg_arg_vals.append(agg_arg_val)
            all_arg_vals.append(agg_arg_vals)

        # 根据哈希key分组
        row_count = len(group_by_vals[0])
        for r in range(row_count):
            hash_key = hash_keys[r]
            #更新聚合函数的中间结果
            self.update_agg_func_val(hash_key,group_by_vals,all_arg_vals,r)

    def Open(self):
        self.child.Open()

    def Next(self):
        records = self.child.Next()
        if records is None:
            return None
        while records is not None:
            # 计算groupby表达式
            group_by_vals = []
            #每行一个hash
            hash_funcs = [hashlib.sha256() for _ in range(records.num_rows)]
            for e in self.groupby:
                val = exec_expr(e,records,0)
                i = 0
                for v in val:
                    hash_funcs[i].update(str(v).encode("utf-8"))
                    i = i + 1
                group_by_vals.append(val)
            hash_keys = []
            for hash in hash_funcs:
                hash_keys.append(hash.hexdigest())
            hash_funcs = None
            self.update_hash_table(group_by_vals,hash_keys,records)
            # 下一批输入
            records = self.child.Next()

        #拼接聚合函数结果
        # groupby1, groupby2, ..., agg1,agg2,...,
        agg_records = self.end_agg_func_val()

        #计算output list的值
        output_vals = []
        output_types = []
        for e in self.output:
            val = exec_expr(e,agg_records,len(self.groupby))
            output_vals.append(val)
            output_types.append((e[1],get_field_from_value(val[0])))

        output_schema = arrow.schema(output_types)
        output_records = arrow.record_batch(output_vals,output_schema)
        return output_records

    def Close(self):
        self.child.Close()
        self.plan = None
        self.child = None
        self.aggregate = None
        self.groupby = None
        self.output = None
        self.hash_func = None
        self.hash_table = None
        self.agg_factory = None

def get_sort_key_name(e,plan):
    #从sortby表达式中取出name
    typ = e[0]
    if typ == EXPR_OP_COLUMN_REF:
        return e[1][0].name
    elif typ == OUTPUT_EXPR_RESULT_REF_EXPR:
        outputs = plan.get(OUTPUT)
        return get_sort_key_name(outputs[e[1]],plan)
    elif typ == OUTPUT_EXPR:
        return e[1]
    else:
        raise Exception(f"not implement sort key name {e}")

def get_sort_keys(orderby,plan):
    sorts =[]
    for e in orderby:
        name = get_sort_key_name(e[1],plan)
        dir = "ascending"
        if e[2] == enums.parsenodes.SortByDir.SORTBY_DESC:
            dir = "descending"
        sorts.append((name,dir))
    return sorts

class OrderbyExecutor(Executor):
    def __init__(self,plan,child):
        super().__init__()
        self.plan = plan
        self.child = child
        self.orderby = plan.get(ORDER,[])

    def Open(self):
        self.child.Open()

    def Next(self):
        records = self.child.Next()
        if records is None:
            return None

        #取排序字段
        sort_keys = get_sort_keys(self.orderby,self.plan)
        #排序并获取排序后的索引
        indices = compute.sort_indices(records,sort_keys)
        #按排序索引重新行顺序
        return compute.take(records,indices)

    def Close(self):
        self.child.Close()
        self.plan = None
        self.child= None
        self.orderby = None

class ProjectListExecutor(Executor):
    def __init__(self,plan,child):
        super().__init__()
        self.plan = plan
        self.child = child
        self.project_list = plan.get(PROJECT,[])

    def Open(self):
        self.child.Open()

    def Next(self):
        records = self.child.Next()
        if records is None:
            return records

        #计算project list的值
        project_vals = []
        project_types = []
        for e in self.project_list:
            val = exec_expr(e,records,0)
            project_vals.append(val)
            project_types.append((e[1],get_field_from_value(val[0])))

        schema = arrow.schema(project_types)
        project_records = arrow.record_batch(project_vals,schema)
        return project_records

    def Close(self):
        self.child.Close()
        self.plan = None
        self.child= None
        self.project_list = None


if __name__ == '__main__':
    plan = {}
    selBuilder = SelectBuilder()
    selBuilder.build(q1Stmt.stmt,plan)
    pprint(plan)

    pplan_builder = PhysicalPlanBuilder()
    exec = pplan_builder.build(plan,PROJECT)
    exec.Open()
    records = exec.Next()
    # csv.write_csv(records,"q1.csv")
    pprint(records.to_pandas())

    exec.Close()