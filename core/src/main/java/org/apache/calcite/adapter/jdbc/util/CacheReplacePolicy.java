package org.apache.calcite.adapter.jdbc.util;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.sql.util.SqlString;

import java.util.*;

/**
 * Multi Layer Replacement policy
 */
public class CacheReplacePolicy {

    public static final int LAYER_COUNT = 3;

    public static final int LAYER_CAPACITY = 5;

    public static final int LAYER_GAP = 3;

    public static Map<QueryInfo, MultiLayerScore> map = new HashMap<>();

    public static Map<QueryInfo, Boolean> cachedMap = new HashMap<>();

    public static Layer firstLayer = new Layer(1, 0, LAYER_GAP, Lists.newArrayList(), null);

    public static boolean isCached(JdbcConvention inputTrait, JdbcConvention outputTrait, SqlString sqlString) {
        return cachedMap.getOrDefault(new QueryInfo(sqlString, inputTrait, outputTrait), true);
    }

    public static void doCacheReplace(JdbcConvention inputTrait, JdbcConvention outputTrait, SqlString sqlString, long migrateTime) {
        QueryInfo queryInfo = new QueryInfo(sqlString, inputTrait, outputTrait);
        MultiLayerScore multiLayerScore;
        if (map.containsKey(queryInfo)) {
            multiLayerScore = map.get(queryInfo);
        } else {
            multiLayerScore = MultiLayerScore.of(queryInfo, 0, migrateTime / 1000000000.);
            map.put(queryInfo, multiLayerScore);
        }
        // update subquery count info
        multiLayerScore.visit(new DummmyRM());
    }

    @AllArgsConstructor
    static class MultiLayerScore implements Comparable<MultiLayerScore> {

        QueryInfo queryInfo;

        double historyScore;

        double costScore;

        Layer layer;

        public static MultiLayerScore of(QueryInfo queryInfo, double historyScore, double costScore) {
            // init layer
            Layer layer = firstLayer;
            while (layer.max < historyScore) {
                layer = layer.nextLayer();
            }
            return new MultiLayerScore(queryInfo, historyScore, costScore, layer);
        }

        public void visit(Visitor visitor) {
            // update history count
            historyScore++;
            // update layer
            if (historyScore >= layer.max) {
                layer.removeElement(this);
            }
            // do replacement
            visitor.visit(this);
        }


        @Override
        public int compareTo(CacheReplacePolicy.MultiLayerScore o) {
            return Double.compare(costScore, o.costScore);
        }
    }

    @AllArgsConstructor
    static class QueryInfo {

        SqlString sqlString;

        JdbcConvention inputTrait;

        JdbcConvention outputTrait;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            QueryInfo queryInfo = (QueryInfo) o;
            return Objects.equals(sqlString, queryInfo.sqlString) &&
                    Objects.equals(inputTrait, queryInfo.inputTrait) &&
                    Objects.equals(outputTrait, queryInfo.outputTrait);
        }

        @Override
        public int hashCode() {
            return Objects.hash(sqlString, inputTrait, outputTrait);
        }
    }

    @AllArgsConstructor
    static class Layer {

        int layerId;

        double min;

        double max;

        List<MultiLayerScore> elements;

        Layer nextLayer;

        public void removeElement(MultiLayerScore score) {
            // todo [coral] use dichotomous
            elements.remove(score);
            nextLayer().addElement(score);
        }

        public void addElement(MultiLayerScore score) {
            elements.add(score);
            Collections.sort(elements);
            if (elements.size() > LAYER_CAPACITY) {
                elements.remove(elements.size() - 1);
            }
        }

        public Layer nextLayer() {
            if (nextLayer == null) {
                if (layerId == LAYER_COUNT - 1) { // final layer
                    nextLayer = new Layer(layerId++, max, Double.MAX_VALUE, Lists.newArrayList(), null);
                } else {
                    nextLayer = new Layer(layerId++, max, max + LAYER_GAP, Lists.newArrayList(), null);
                }
            }
            return nextLayer;
        }

    }

    interface Visitor {
        void visit(MultiLayerScore multiLayerScore);
    }

    static class TableRM implements Visitor {

        @Override
        public void visit(MultiLayerScore multiLayerScore) {
            // do table remove
        }
    }

    static class DummmyRM implements Visitor {

        @Override
        public void visit(MultiLayerScore multiLayerScore) {
//            cachedMap.put(multiLayerScore.queryInfo, false);
        }
    }

}
