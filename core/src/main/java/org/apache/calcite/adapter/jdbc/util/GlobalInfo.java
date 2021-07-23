package org.apache.calcite.adapter.jdbc.util;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.dqn.CostModel;
import org.apache.calcite.plan.dqn.OptimizerMLP;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mainly for store conventions and table attributes info && provide the unique id for these info
 * <p>currently is used for generate one-hot code for dqn optimizer </p>
 * todo [coral] record table attr info to global space when system start
 *
 * @see org.apache.calcite.plan.dqn.EpisodeSample
 */
@Slf4j
public class GlobalInfo {

//    @Getter
//    private static ExecutorService executorService = Executors.newCachedThreadPool();

    static {
        // preload spark
        SparkHandler.instance();
        // preload
        try {
            OptimizerMLP.train(CostModel.COST_MODEL_1);
        } catch (IOException e) {
            log.error("net work init failed");
        }
    }

    @Getter
    private static AtomicInteger queryCount = new AtomicInteger(); // for update isDataReady every n query

    @Getter
    private ThreadLocal<CalciteConnectionConfig> configThreadLocal = new ThreadLocal<>();

    private Map<String, Integer> attrIndexMap = new HashMap<>();
    private Map<String, Integer> conventionIndexMap = new HashMap<>();
    private Map<String, Convention> conventionMap = new HashMap<>();

    public Convention getFixedPlatform() {
        CalciteConnectionConfig config = getConfigThreadLocal().get();
        if (config.fixedPlatform() != null) {
            return getConvention(config.fixedPlatform());
        }
        return null;
    }

    public void addAttr(String attrName) {
        attrIndexMap.put(attrName, attrIndexMap.size());
    }

    public int getAttrIndex(String attrName) {
        return attrIndexMap.get(attrName);
    }

    public int getAttrSize() {
        return attrIndexMap.size();
    }

    public void addConvention(Convention convention) {
        conventionIndexMap.putIfAbsent(convention.getName(), conventionIndexMap.size());
        conventionMap.put(convention.getName(), convention);
    }

    public int getConventionIndex(String conventionName) {
        return conventionIndexMap.get(conventionName);
    }

    public int getConventionSize() {
        return conventionIndexMap.size();
    }

    public Convention getConvention(String conventionName) {
        return conventionMap.get(conventionName);
    }

    public int getFeatureSize() {
        return getAttrSize() * 3 + getConventionSize();
    }

    private static class Holder {
        private static final GlobalInfo INSTANCE = new GlobalInfo();
    }

    public static GlobalInfo getInstance() {
        return Holder.INSTANCE;
    }

    private GlobalInfo() {
        addAttr("o_orderkey");
        addAttr("o_custkey");
        addAttr("o_orderstatus");
        addAttr("o_totalprice");
        addAttr("o_orderdate");
        addAttr("o_orderpriority");
        addAttr("o_clerk");
        addAttr("o_shippriority");
        addAttr("o_comment");

        addAttr("l_orderkey");
        addAttr("l_partkey");
        addAttr("l_suppkey");
        addAttr("l_linenumber");
        addAttr("l_quantity");
        addAttr("l_extendedprice");
        addAttr("l_discount");
        addAttr("l_tax");
        addAttr("l_returnflag");
        addAttr("l_linestatus");
        addAttr("l_shipdate");
        addAttr("l_commitdate");
        addAttr("l_receiptdate");
        addAttr("l_shipinstruct");
        addAttr("l_shipmode");
        addAttr("l_comment");

        addAttr("ps_partkey");
        addAttr("ps_suppkey");
        addAttr("ps_availqty");
        addAttr("ps_supplycost");
        addAttr("ps_comment");

        addAttr("c_custkey");
        addAttr("c_name");
        addAttr("c_address");
        addAttr("c_nationkey");
        addAttr("c_phone");
        addAttr("c_acctbal");
        addAttr("c_mktsegment");
        addAttr("c_comment");

        addAttr("n_nationkey");
        addAttr("n_name");
        addAttr("n_regionkey");
        addAttr("n_comment");

        addAttr("r_regionkey");
        addAttr("r_name");
        addAttr("r_comment");

        addAttr("p_partkey");
        addAttr("p_name");
        addAttr("p_mfgr");
        addAttr("p_brand");
        addAttr("p_type");
        addAttr("p_size");
        addAttr("p_container");
        addAttr("p_retailprice");
        addAttr("p_comment");

        addAttr("s_suppkey");
        addAttr("s_name");
        addAttr("s_address");
        addAttr("s_nationkey");
        addAttr("s_phone");
        addAttr("s_acctbal");
        addAttr("s_comment");
    }

}
