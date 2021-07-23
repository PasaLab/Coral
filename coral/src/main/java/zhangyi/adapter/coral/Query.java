package zhangyi.adapter.coral;

import lombok.Getter;

@Getter
public class Query {

    //    public static final String Q1 = "SELECT n.n_name, s.s_suppkey, l.l_orderkey FROM postgres.nation as n \n" +
//            "join memsql.supplier as s on s.s_nationkey = n.n_nationkey \n" +
//            "join clickhouse.lineitem as l on l.l_suppkey = s.s_suppkey \n" +
//            "where n.n_nationkey = 5 \n" +
//            "and s.s_suppkey < 400 \n" +
//            "and l.l_orderkey < 5000 ";
    //    public static final String Q2 = "SELECT p_partkey, s_suppkey, ps_supplycost FROM clickhouse.partsupp as ps \n" +
//            "join memsql.supplier as s on s.s_suppkey  = ps.ps_suppkey \n" +
//            "join memsql.part as p on p.p_partkey = ps.ps_partkey \n" +
//            "where p.p_partkey < 400 \n" +
//            "and s.s_suppkey < 400 ";
    public static final String Q1 = "SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM clickhouse.orders as o \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join postgres.customer as c on o.o_custkey = c.c_custkey \n" +
            "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey \n" +
            "where c.c_nationkey = 3 \n" +
            "and s.s_suppkey = 1000";
    public static final String Q2 = "SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM clickhouse.orders as o \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join postgres.customer as c on o.o_custkey = c.c_custkey \n" +
            "join memsql.part as p on p.p_partkey = l.l_partkey \n" +
            "where c.c_nationkey = 5 \n" +
            "and p.p_partkey = 1000";
    public static final String Q3 = "SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM clickhouse.orders as o \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join postgres.customer as c on o.o_custkey = c.c_custkey \n" +
            "join memsql.part as p on p.p_partkey = l.l_partkey \n" +
            "where c.c_custkey = 1000 \n" +
            "and p.p_partkey = 1000";
    public static final String Q4 = "SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join memsql.part as p on p.p_partkey = l.l_partkey \n" +
            "where c.c_nationkey = 3 \n" +
            "and o.o_orderkey < 500 \n" +
            "and l.l_orderkey < 500 ";
    public static final String Q5 = "SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey \n" +
            "where c.c_nationkey = 5 \n" +
            "and o.o_orderkey < 500 \n" +
            "and l.l_orderkey < 500 ";
    public static final String Q6 = "SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join memsql.part as p on p.p_partkey = l.l_partkey \n" +
            "where c.c_custkey = 1000 \n" +
            "and o.o_orderkey < 500 \n" +
            "and l.l_orderkey < 500 ";
    public static final String Q7 = "SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join memsql.part as p on p.p_partkey = l.l_partkey \n" +
            "where o.o_orderkey < 500 \n" +
            "and l.l_orderkey < 500 \n" +
            "and p.p_partkey < 10000 ";
    public static final String Q8 = "SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey \n" +
            "where o.o_orderkey < 500 \n" +
            "and l.l_orderkey < 500 \n" +
            "and s.s_suppkey < 10000 ";
    public static final String Q9 = "SELECT o_custkey, c_name, c_nationkey, p_name FROM postgres.customer as c \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join memsql.part as p on p.p_partkey = l.l_partkey \n" +
            "where o.o_orderkey < 500 \n" +
            "and l.l_orderkey < 500 \n" +
            "and p.p_size = 1 ";
    public static final String Q10 = "SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM clickhouse.partsupp as ps \n" +
            "join memsql.supplier as s on ps.ps_suppkey = s.s_suppkey \n" +
            "join postgres.nation as n on s.s_nationkey = n.n_nationkey \n" +
            "where n.n_nationkey = 5 \n";
    public static final String Q11 = "SELECT c_custkey, c_nationkey, n_name, o_orderkey FROM postgres.customer as c " +
            "join postgres.nation as n on n.n_nationkey = c.c_nationkey " +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
            "where n.n_nationkey = 1 ";
    public static final String Q12 = "SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c \n" +
            "join postgres.nation as n on n.n_nationkey = c.c_nationkey \n" +
            "join postgres.region as r on r.r_regionkey = n.n_regionkey \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join memsql.part as p on p.p_partkey = l.l_partkey \n" +
            "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey \n" +
            "where c.c_nationkey < 5 \n" +
            "and o.o_orderkey < 1000 \n" +
            "and l.l_orderkey < 1000 ";
    // join number 1
    public static final String Q13 = "SELECT n_name FROM postgres.nation as n \n" +
            "where n.n_nationkey = 5 ";
    // join number 2
    public static final String Q14 = "SELECT n_name, s_suppkey FROM postgres.nation as n \n" +
            "join memsql.supplier as s on s.s_nationkey = n.n_nationkey \n" +
            "where s.s_suppkey < 50000 \n";
    // join number 8
    public static final String Q15 = "SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c \n" +
            "join postgres.nation as n on n.n_nationkey = c.c_nationkey \n" +
            "join postgres.region as r on r.r_regionkey = n.n_regionkey \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join clickhouse.partsupp as ps on ps.ps_partkey = l.l_partkey \n" +
            "join memsql.part as p on p.p_partkey = l.l_partkey \n" +
            "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey \n" +
            "where c.c_nationkey < 5 \n" +
            "and o.o_orderkey < 1000 \n" +
            "and l.l_orderkey < 1000 \n" +
            "and ps.ps_partkey < 200 ";
    // join number 5
    public static final String Q16 = "SELECT c_custkey, c_nationkey, o_orderkey, l_suppkey FROM postgres.customer as c " +
            "join postgres.nation as n on n.n_nationkey = c.c_nationkey " +
            "join postgres.region as r on r.r_regionkey = n.n_regionkey " +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
            "where c.c_nationkey < 5 "
            + "and o.o_orderkey < 1000 "
            + "and l.l_orderkey < 1000 ";
    // join number 6
    public static final String Q17 = "SELECT c_custkey, c_nationkey, o_orderkey, l_suppkey FROM postgres.customer as c \n" +
            "join postgres.nation as n on n.n_nationkey = c.c_nationkey \n" +
            "join postgres.region as r on r.r_regionkey = n.n_regionkey \n" +
            "join clickhouse.orders as o on o.o_custkey = c.c_custkey \n" +
            "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey \n" +
            "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey \n" +
            "where c.c_nationkey < 5 \n" +
            "and o.o_orderkey < 1000 \n" +
            "and l.l_orderkey < 1000 \n" +
            "and s.s_suppkey < 500 ";
    // test multi-way join
    public static final String Q18 = "SELECT n.n_name, s.s_suppkey, l.l_orderkey FROM postgres.nation as n \n" +
            ", memsql.supplier as s  \n" +
            ", clickhouse.lineitem as l \n" +
            "where s.s_nationkey = n.n_nationkey " +
            "and l.l_suppkey = s.s_suppkey " +
            "and n.n_nationkey = 5 \n" +
            "and s.s_suppkey < 400 \n" +
            "and l.l_orderkey < 5000 ";
    public static final String Q19 = "SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n \n" +
            "join memsql.supplier as s on s.s_nationkey = n.n_nationkey \n" +
            "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey \n" +
            "where n.n_nationkey = 5 \n" +
            "and ps.ps_suppkey < 4000 ";
}
