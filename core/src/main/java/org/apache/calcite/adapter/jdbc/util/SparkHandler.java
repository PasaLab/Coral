package org.apache.calcite.adapter.jdbc.util;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

@Slf4j
public class SparkHandler {

    public SparkSession spark;

    /**
     * Thread-safe holder
     */
    private static class Holder {
        private static final SparkHandler INSTANCE = new SparkHandler();
    }

    /**
     * Creates a SparkHandlerImpl.
     */
    private SparkHandler() {
        log.info("init spark, master node = {}, jar path = {}, executor mem = {}",
                CalciteSystemProperty.SPARK_MASTER_NODE.value(), CalciteSystemProperty.SPARK_JAR_PATH.value(), CalciteSystemProperty.SPARK_EXECUTOR_MEM.value());
        SparkConf conf = new SparkConf().setAppName("coral")
                .setMaster(CalciteSystemProperty.SPARK_MASTER_NODE.value())
                .setJars(new String[]{CalciteSystemProperty.SPARK_JAR_PATH.value()})
                .set("spark.executor.memory", CalciteSystemProperty.SPARK_EXECUTOR_MEM.value());
        spark = SparkSession.builder().config(conf).getOrCreate();
    }

    /**
     * Creates a SparkHandlerImpl, initializing on first call. Calcite-core calls
     * this via reflection.
     */
    @SuppressWarnings("UnusedDeclaration")
    public static SparkHandler instance() {
        return Holder.INSTANCE;
    }
}
