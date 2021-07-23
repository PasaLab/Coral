package org.apache.calcite.plan.musqle;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcToJdbcConverterRule;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;
import java.util.Set;

public class MuSQLEPlanner {

    public static RelNode optimize(RelNode rootRel,
                                   RelBuilder relBuilder,
                                   RelTraitSet traitSet) {
        // find the best engine for query
        RelBuilderFactory relBuilderFactory = RelFactories.LOGICAL_BUILDER;
        final RelMetadataQuery mq = rootRel.getCluster().getMetadataQuery();
        Set<Convention> conventions = Sets.newHashSet();
        List<RelOptRule> rules = Lists.newArrayList();
        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (node instanceof TableScan) {
                    conventions.add(node.getConvention());
                    rules.addAll(JdbcRules.rules((JdbcConvention) node.getConvention()));
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(rootRel);
        RelNode best = null;
        RelOptCost minCost = null;
        for (Convention convention : conventions) {
            List<RelOptRule> currentRules = Lists.newArrayList(rules);
            for (Convention sourceConvention : conventions) {
                if (!convention.equals(sourceConvention)) {
                    currentRules.add(new JdbcToJdbcConverterRule((JdbcConvention) sourceConvention, (JdbcConvention) convention, relBuilderFactory));
                }
            }
            Program program = Programs.ofRules(currentRules);
            RelOptPlanner planner = rootRel.getCluster().getPlanner();
            RelNode optimized = program.run(planner, rootRel, traitSet, Lists.newArrayList(), Lists.newArrayList());
            RelOptCost cumulativeCost = mq.getCumulativeCost(optimized);
            if (minCost == null || cumulativeCost.isLt(minCost)) {
                minCost = cumulativeCost;
                best = optimized;
            }
        }
        return best;
    }

}
