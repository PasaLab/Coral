package zhangyi.adapter.coral;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.config.Lex;
import org.apache.calcite.test.CalciteAssert;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TestUtil;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Consumer;

import static zhangyi.adapter.coral.Query.*;

@SuppressWarnings("DuplicatedCode")
@Slf4j
public class CoralExperimentBox {

    private static final String sqlSuffix = " limit 10 ";

    private static String local = null;

    private static final String[] P0_P3 = {
            "JDBC.clickhouse",
            "JDBC.memsql",
            "JDBC.postgresql",
            "NONE.NONE",
    };

    private static final String[] Q1_Q9 = {Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9};

    private static final String[] Q1_Q12 = {Q1, Q2, Q3, Q4, Q5, Q6, Q7, Q8, Q9, Q10, Q11, Q12};

    private static final String[] J1_J8 = {Q13, Q14, Q19, Q6, Q16, Q17, Q12, Q15};

    private static final String[] H = {Q1, Q4, Q7, Q7, Q7, Q5, Q9, Q10, Q13, Q14, Q15, Q16, Q17};

    public static void main(String[] args) {
//        local = "model-localhost.json";
        doExperiment(args[0]);
//        doExperiment("1-1");
//        doExperiment("1-2");
//        doExperiment("1-3");
//        doExperiment("2-1");
    }

    private static void doExperiment(String name) {
        switch (name) {
            case "1-1":
                E1_1();
                break;
            case "1-2":
                E1_2();
                break;
            case "1-3":
                E1_3();
                break;
            case "2-1":
                E2_1();
                break;
            case "3-1":
                E3_1();
                break;
            case "3-2":
                E3_2();
                break;
            case "3-3":
                E3_3();
                break;
        }
    }

    private static void E1_1() {
        String model = local == null ? "model-10G.json" : local;
        for (String platform : P0_P3) {
            for (int i = 0; i < Q1_Q9.length; i++) {
                log.info("Current Query = Q{}, Platform = {}", i + 1, platform);
                if (i < 3 && (platform.equals("JDBC.memsql") || platform.equals("JDBC.postgresql"))) {
                    continue; // Q1 - Q3 clickhouse数据太多，这两种平台绑定必定超时
                }
                CalciteAssert.that()
                        .with(CalciteConnectionProperty.MODEL, CalciteSystemProperty.CORAL_CONF_DIR.value() + model)
                        .with(Lex.JAVA)
                        .with(CalciteConnectionProperty.FIXED_PLATFORM, platform)
                        .with(CalciteConnectionProperty.CACHE_ENABLED, false)
                        .query(Q1_Q9[i] + sqlSuffix)
                        .returns((Consumer<ResultSet>) CoralExperimentBox::output);
            }
        }
    }

    private static void E1_2() {
        String model = local == null ? "model-50G.json" : local;
        for (int i = 0; i < J1_J8.length; i++) {
            log.info("Current Query = J{}", i + 1);
            CalciteAssert.that()
                    .with(CalciteConnectionProperty.MODEL, CalciteSystemProperty.CORAL_CONF_DIR.value() + model)
                    .with(Lex.JAVA)
                    .with(CalciteConnectionProperty.CACHE_ENABLED, false)
                    .query(J1_J8[i] + sqlSuffix)
                    .returns((Consumer<ResultSet>) CoralExperimentBox::output);
        }
    }

    private static void E1_3() {
        String model = local == null ? "model-50G.json" : local;
        List<Pair<Integer, Integer>> thresholds = Lists.newArrayList(
                Pair.of(100, 6),  // Standard
                Pair.of(1, 4), // DQN
                Pair.of(4, 4) // Hybrid
        );
        List<String> OptimizerName = Lists.newArrayList("Standard", "DNQ", "Hybrid");
        for (int i = 0; i < thresholds.size(); i++) {
            for (int j = 0; j < J1_J8.length; j++) {
                log.info("Current Query = J{}, Optimizer = {}", j + 1, OptimizerName.get(i));
                CalciteAssert.that()
                        .with(CalciteConnectionProperty.MODEL, CalciteSystemProperty.CORAL_CONF_DIR.value() + model)
                        .with(Lex.JAVA)
                        .with(CalciteConnectionProperty.CACHE_ENABLED, false)
                        .with(CalciteConnectionProperty.DQN_THRESHOLD, thresholds.get(i).left)
                        .with(CalciteConnectionProperty.HEURISTIC_THRESHOLD, thresholds.get(i).right)
                        .query(J1_J8[j] + sqlSuffix)
                        .returns((Consumer<ResultSet>) CoralExperimentBox::output);
            }
        }

    }

    private static void E2_1() { // clean space before do experiment
        String model = local == null ? "model-50G.json" : local;
        // do history
        for (int i = 0; i < H.length; i++) {
            log.info("Current Query = H{}", i + 1);
            CalciteAssert.that()
                    .with(CalciteConnectionProperty.MODEL, CalciteSystemProperty.CORAL_CONF_DIR.value() + model)
                    .with(Lex.JAVA)
                    .query(H[i] + sqlSuffix)
                    .returns((Consumer<ResultSet>) CoralExperimentBox::output);
        }
        List<Boolean> cached = Lists.newArrayList(true, false);
        for (Boolean c : cached) {
            for (int i = 0; i < Q1_Q12.length; i++) {
                log.info("Current Query = Q{}, CacheEnabled = {}", i + 1, c);
                CalciteAssert.that()
                        .with(CalciteConnectionProperty.MODEL, CalciteSystemProperty.CORAL_CONF_DIR.value() + model)
                        .with(Lex.JAVA)
                        .with(CalciteConnectionProperty.CACHE_ENABLED, c)
                        .query(Q1_Q12[i] + sqlSuffix)
                        .returns((Consumer<ResultSet>) CoralExperimentBox::output);
            }
        }
    }

    private static void E3_1() {
        String model = local == null ? "model-10G.json" : local;
        for (int i = 0; i < Q1_Q12.length; i++) {
            log.info("Current Query = Q{}", i + 1);
            CalciteAssert.that()
                    .with(CalciteConnectionProperty.MODEL, CalciteSystemProperty.CORAL_CONF_DIR.value() + model)
                    .with(Lex.JAVA)
                    .query(Q1_Q12[i] + sqlSuffix)
                    .returns((Consumer<ResultSet>) CoralExperimentBox::output);
        }
    }

    private static void E3_2() {
        String model = local == null ? "model-50G.json" : local;
        for (int i = 0; i < Q1_Q12.length; i++) {
            log.info("Current Query = Q{}", i + 1);
            CalciteAssert.that()
                    .with(CalciteConnectionProperty.MODEL, CalciteSystemProperty.CORAL_CONF_DIR.value() + model)
                    .with(Lex.JAVA)
                    .query(Q1_Q12[i] + sqlSuffix)
                    .returns((Consumer<ResultSet>) CoralExperimentBox::output);
        }
    }

    private static void E3_3() {
        String model = local == null ? "model-100G.json" : local;
        for (int i = 0; i < Q1_Q12.length; i++) {
            log.info("Current Query = Q{}", i + 1);
            CalciteAssert.that()
                    .with(CalciteConnectionProperty.MODEL, CalciteSystemProperty.CORAL_CONF_DIR.value() + model)
                    .with(Lex.JAVA)
                    .query(Q1_Q12[i] + sqlSuffix)
                    .returns((Consumer<ResultSet>) CoralExperimentBox::output);
        }
    }

    private static Void output(ResultSet resultSet) {
        try {
            output(resultSet, System.out);
        } catch (SQLException e) {
            throw TestUtil.rethrow(e);
        }
        return null;
    }

    private static void output(ResultSet resultSet, PrintStream out)
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