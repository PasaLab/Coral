package org.apache.calcite.plan.sloth;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;

import java.util.List;
import java.util.Stack;

public class SlothPlanner {


    public static RelNode optimize(RelNode rootRel,
                                   RelBuilder relBuilder,
                                   RelTraitSet traitSet) {
        // 1. enumerate join orders
        // 1.1 init vertex
        List<Vertex> vertex = Lists.newArrayList();
        Stack<Join> joinStack = new Stack<>();
        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) { // 中序遍历
                if (node instanceof Join) {
                    if (!joinStack.isEmpty()) {
                        joinStack.pop();
                    }
                    joinStack.push((Join) node);
                    joinStack.push((Join) node);
                }
                if (node instanceof TableScan) {
                    Join lastJoin = joinStack.pop();
                    if (!joinStack.isEmpty() && joinStack.peek() == lastJoin) {
                        vertex.add(new Vertex(lastJoin.getLeft(), node.getConvention()));
                    } else {
                        vertex.add(new Vertex(lastJoin.getRight(), node.getConvention()));
                    }
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(rootRel);
        // 1.2 use dp to reorder


        // 2. for each join order, use dp to calculate the best cost
        List<RelOptRule> rules = com.clearspring.analytics.util.Lists.newArrayList();
        Program program = Programs.ofRules(rules);
        RelOptPlanner planner = rootRel.getCluster().getPlanner();
        RelNode optimized = program.run(planner, rootRel, traitSet, com.clearspring.analytics.util.Lists.newArrayList(), com.clearspring.analytics.util.Lists.newArrayList());

        // 3. return the min cost plan

        return null;
    }


    @AllArgsConstructor
    static class Vertex {
        RelNode node;
        Convention convention;

        public static Vertex of(RelNode node, Convention convention) {
            return new Vertex(node, convention);
        }

        public static Vertex of(RelNode node) {
            return new Vertex(node, node.getConvention());
        }
    }

}
