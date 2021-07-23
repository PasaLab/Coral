import com.google.common.collect.ImmutableMap;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Sources;
import org.apache.calcite.util.TestUtil;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.function.Consumer;

import static org.junit.Assume.assumeTrue;
import static zhangyi.adapter.coral.Query.*;

public class CoralTest {

    /**
     * Connection factory based on the "clickhouse + postgres + memsql" model.
     */
    private static final ImmutableMap<String, String> CONFIG =
            ImmutableMap.of(
                    "model",
                    Sources.of(
                            CoralTest.class.getResource("/model-localhost.json"))
//                            CalciteTest.class.getResource("/model-10G.json"))
                            .file().getAbsolutePath());


    private void execute(String sql) {
        String sqlSuffix = " limit 10 ";
        CalciteAssert.that()
                .with(CONFIG)
                .with(Lex.JAVA)
                .with(CalciteConnectionProperty.CACHE_ENABLED, false)
//                .with(CalciteConnectionProperty.FIXED_PLATFORM, "JDBC.clickhouse")
//                .with(CalciteConnectionProperty.FIXED_PLATFORM, "JDBC.memsql")
//                .with(CalciteConnectionProperty.FIXED_PLATFORM, "JDBC.postgresql")
//                .with(CalciteConnectionProperty.DQN_THRESHOLD, 100)
//                .with(CalciteConnectionProperty.HEURISTIC_THRESHOLD, 100)
                .query(sql + sqlSuffix)
                .returns((Consumer<ResultSet>) this::output);
    }

    private static boolean enabled() {
        return true;
    }

    @BeforeClass
    public static void setUp() {
        // run tests only if explicitly enabled
        assumeTrue("test explicitly disabled", enabled());
        // run history
    }

    @Test
    public void tpch_Q1_IN_CLICKHOUSE() {
        execute(Q1);
    }

    @Test
    public void tpch_Q2_IN_CLICKHOUSE() {
        execute(Q2);
    }

    @Test
    public void tpch_Q3_IN_CLICKHOUSE() {
        execute(Q3);
    }

    @Test
    public void tpch_Q4_IN_MEMSQL() {
        execute(Q4);
    }

    @Test
    public void tpch_Q5_IN_MEMSQL() {
        execute(Q5);
    }

    @Test
    public void tpch_Q6_IN_MEMSQL() {
        execute(Q6);
    }

    @Test
    public void tpch_Q7_IN_POSTGRESQL() {
        execute(Q7);
    }

    @Test
    public void tpch_Q8_IN_POSTGRESQL() {
        execute(Q8);
    }

    @Test
    public void tpch_Q9_IN_POSTGRESQL() {
        execute(Q9);
    }

    @Test
    public void tpch_Q10_IN_POSTGRESQL_CLICKHOUSE() {
        execute(Q10);
    }

    @Test
    public void tpch_Q11_IN_MEMSQL_CLICKHOUSE() {
        execute(Q11);
    }

    @Test
    public void tpch_Q12_IN_POSTGRE_CLICKHOUSE_MEMSQL() {
        execute(Q12);
    }

    @Test
    public void tpch_Q13_IN_POSTGRE() {
        execute(Q13);
    }

    @Test
    public void tpch_Q14_IN_CLICKHOUSE() {
        execute(Q14);
    }

    @Test
    public void tpch_Q15_IN_CLICKHOUSE_MEMSQL_POSTGRE() {
        execute(Q15);
    }

    @Test
    public void tpch_Q16_IN_CLICKHOUSE_POSTGRE() {
        execute(Q16);
    }

    @Test
    public void tpch_Q17_IN_MEMSQL_POSTGRE() {
        execute(Q17);
    }

    @Test
    public void tpch_Q18_IN_MEMSQL_POSTGRE() {
        execute(Q18);
    }


//    @Test
//    public void test(){
//        execute("SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c " +
//                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
//                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
//                "join memsql.part as p on p.p_partkey = l.l_partkey " +
//                "where c.c_custkey <= 10 "
//                + "and p.p_partkey = 17008 ");
//    }

    public void HISTORY() {
        execute("SELECT o_custkey, c_name, c_nationkey, p_name FROM postgres.customer as c, " +
                "clickhouse.orders as o, " +
                "clickhouse.lineitem as l, " +
                "memsql.part as p " +
                "where o.o_custkey = c.c_custkey " +
                "and l.l_orderkey = o.o_orderkey " +
                "and p.p_partkey = l.l_partkey "
                + "and c.c_custkey <= 10 "
                + "and o.o_custkey <= 10 "
                + "and l.l_orderkey < 40000 ");
        execute("SELECT c_custkey, c_nationkey, o_orderkey, p_partkey, p_name FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.part as p on p.p_partkey = l.l_partkey " +
                "where c.c_custkey <= 10 "
                + "and p.p_partkey = 17008 ");
        execute("SELECT c_custkey, c_nationkey, o_orderkey, s_suppkey FROM postgres.customer as c " +
                "join clickhouse.orders as o on o.o_custkey = c.c_custkey " +
                "join clickhouse.lineitem as l on l.l_orderkey = o.o_orderkey " +
                "join memsql.supplier as s on s.s_suppkey  = l.l_suppkey " +
                "where o.o_orderkey < 500 "
                + "and l.l_orderkey < 500 "
                + "and s.s_suppkey < 10000 ");
        execute("SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and ps.ps_suppkey < 200 ");
        execute("SELECT p_partkey, s_suppkey, ps_supplycost FROM clickhouse.partsupp as ps " +
                "join memsql.supplier as s on s.s_suppkey  = ps.ps_suppkey " +
                "join memsql.part as p on p.p_partkey = ps.ps_partkey " +
                "where p.p_partkey < 400 "
                + "and s.s_suppkey < 400 ");
        execute("SELECT n.n_name, s.s_suppkey, l.l_orderkey FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.lineitem as l on l.l_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and s.s_suppkey < 400 ");
        execute("SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and ps.ps_suppkey < 200 ");
        execute("SELECT n.n_name, s.s_suppkey, ps.ps_comment FROM postgres.nation as n " +
                "join memsql.supplier as s on s.s_nationkey = n.n_nationkey " +
                "join clickhouse.partsupp as ps on ps.ps_suppkey = s.s_suppkey " +
                "where n.n_nationkey = 5 "
                + "and ps.ps_suppkey < 200 ");
    }

    private Void output(ResultSet resultSet) {
        try {
            output(resultSet, System.out);
        } catch (SQLException e) {
            throw TestUtil.rethrow(e);
        }
        return null;
    }

    private void output(ResultSet resultSet, PrintStream out)
            throws SQLException {
        final ResultSetMetaData metaData = resultSet.getMetaData();
        final int columnCount = metaData.getColumnCount();
        out.println("coral query result: ");
        for (int i = 1; i <= columnCount; i++) {
            out.print(metaData.getColumnName(i) + "\t");
        }
        out.println();
        while (resultSet.next()) {
            for (int i = 1; ; i++) {
                out.print(resultSet.getString(i));
                if (i < columnCount) {
                    out.print(", ");
                } else {
                    out.println();
                    break;
                }
            }
        }
    }

}