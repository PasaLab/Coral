package org.apache.calcite.plan.dqn;

import com.clearspring.analytics.util.Lists;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.jdbc.JdbcConvention;
import org.apache.calcite.adapter.jdbc.JdbcRules;
import org.apache.calcite.adapter.jdbc.JdbcToJdbcConverterRule;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.*;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.mapping.IntPair;
import org.nd4j.linalg.api.ndarray.INDArray;
import org.nd4j.linalg.factory.Nd4j;

import java.util.List;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * for volcano: hold RelNode <-> GraphNode
 * for rl: hold GraphNode <-> Graph, actionSpace
 */
@Slf4j
public class EpisodeSample implements IMDP, ITransformer {

    private Graph graph;

    private CostModel costModel;

    EpisodeSample(Graph graph, CostModel costModel) {
        this.graph = graph;
        this.costModel = costModel;
    }

    @Override
    public Graph getGraph() {
        return graph;
    }

    @Override
    public List<Action> getActionSpace() {
        return graph.edge.stream().flatMap(x -> {
            List<Action> actions = Lists.newArrayList();
            actions.add(new Action(x.left, x.right, x, x.left.convention)); // no need to exchange the left and right
            actions.add(new Action(x.left, x.right, x, x.right.convention));
            return actions.stream();
        }).collect(Collectors.toList());
    }

    @Override
    public boolean isDone() {
        return graph.vertex.size() == 1;
    }

    @Override
    public void takeAction(Action action) {
        // 1. rm two vertex in the graph
        graph.vertex.remove(action.left);
        graph.vertex.remove(action.right);
        // 2. add new vertex to the graph
        Vertex newVertex = action.toNewVertex();
        graph.vertex.add(newVertex);
        // 3. delete chosen edge && update affected edge
        graph.edge.remove(action.edge);
        graph.edge.forEach(edge -> {
            if (edge.left == action.left || edge.left == action.right) {
                edge.left = newVertex;
            } else if (edge.right == action.left || edge.right == action.right) {
                edge.right = newVertex;
            }
        });
    }

    @Override
    public RelNode mdp2RelNode() {
//        graph.root.copy() // add nodes before first join
        RelNode node = graph.vertex.get(0).node;
        List<RelOptRule> rules = Lists.newArrayList();
        rules.addAll(JdbcRules.rules((JdbcConvention) node.getConvention()));
        Program program = Programs.ofRules(rules);
        RelOptPlanner planner = node.getCluster().getPlanner();
        RelTraitSet traitSet = planner.emptyTraitSet().replace(EnumerableConvention.INSTANCE);
        return program.run(planner, node, traitSet, Lists.newArrayList(), Lists.newArrayList());
    }

    @Override
    public void mdp2DataFile() {
        // retrieve action sequence
        Stack<Join> joinStack = new Stack<>();
        List<Action> actions = Lists.newArrayList();
        final RelVisitor visitor = new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) { // 中序遍历
                if (node instanceof Join) {
                    joinStack.push((Join) node);
                    joinStack.push((Join) node);
                }
                if (node instanceof TableScan) {
                    Join lastJoin = joinStack.pop();
                    while (joinStack.isEmpty() || joinStack.peek() != lastJoin) {
                        actions.add(new Action(Vertex.of(lastJoin.getLeft()), Vertex.of(lastJoin.getRight()), null, lastJoin.getConvention()));
                        if (joinStack.isEmpty()) {
                            break;
                        }
                        lastJoin = joinStack.pop();
                    }
                }
                super.visit(node, ordinal, parent);
            }
        };
        visitor.go(graph.vertex.get(0).node);
        // save ([graph vector + action vector] -> cost) into file
        for (Action action : actions) {
            INDArray input = Nd4j.concat(1, graph.toVector(), action.toVector());
            RelMetadataQuery metadataQuery = graph.root.getCluster().getMetadataQuery();
            double cost = metadataQuery.getCumulativeCost(graph.root).getRows()
                    - metadataQuery.getCumulativeCost(action.left.node).getRows()
                    - metadataQuery.getCumulativeCost(action.right.node).getRows();
            IOUtil.writeTrainingData(input, cost, costModel);
        }
    }

    /**
     * build EpisodeSample instance from rel and it's costModel
     *
     * @param relNode   physical plan
     * @param costModel cost model which determines the mlp model name
     * @return new instance
     */
    public static EpisodeSample rel2MDP(RelNode relNode, boolean isPhysical, CostModel costModel) {
        // set graph
        Graph graph = new Graph(relNode, isPhysical);
        return new EpisodeSample(graph, costModel);
    }


    static class Graph {

        RelNode root;
        List<Vertex> vertex;
        List<Edge> edge;

        Graph(RelNode root, boolean isPhysical) {
            this.root = root;
            this.vertex = Lists.newArrayList();
            this.edge = Lists.newArrayList();
            if (isPhysical) {
                this.vertex.add(new Vertex(root, root.getConvention()));
            } else {
                initVertex();
                initEdge();
            }
        }

        INDArray toVector() {            // make sure global row type is registered
            return IOUtil.relNode2Vector(root);
        }

        /**
         * presume Join's children are these conditions (not physical)
         * <ol>
         * <li> join (recursive) </li>
         * <li> project -> filter -> tableScan </li>
         * <li> filter -> project -> tableScan </li>
         * <li> filter -> tableScan </li>
         * <li> project -> tableScan
         * </ol>
         * this function find the RelNode represent 2,3,4,5
         */
        private void initVertex() {
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
            visitor.go(root);
        }

        /**
         * Presumption
         * <ol>
         * <li>each join has multi equal condition.
         * <p>Case 1 (single): A join B on A.x = B.y</p>
         * <p>Case 2 (multi): A join B on A.x1 = B.y1 and A.x2 = B.y2</p>
         * </li>
         * <li>
         * field name in different table should be different // todo [coral] maybe support auto table prefix afterwards
         * </li>
         * </ol>
         */
        private void initEdge() {
            final RelVisitor visitor = new RelVisitor() {
                @Override
                public void visit(RelNode node, int ordinal, RelNode parent) { // 中序遍历
                    // record the join as edge
                    if (node instanceof Join) {
                        Join join = ((Join) node);
                        JoinInfo joinInfo = join.analyzeCondition();
                        // retrieve join fields
                        List<IntPair> pairs = joinInfo.pairs();
                        for (IntPair pair : pairs) {
                            // find vertex with that field
                            RelDataTypeField leftField = join.getLeft().getRowType().getFieldList().get(pair.source);
                            RelDataTypeField rightField = join.getRight().getRowType().getFieldList().get(pair.target);
                            Vertex left = null;
                            Vertex right = null;
                            for (Vertex currVertex : vertex) {
                                if (currVertex.node.getRowType().getFieldNames().contains(leftField.getName())) {
                                    left = currVertex;
                                }
                                if (currVertex.node.getRowType().getFieldNames().contains(rightField.getName())) {
                                    right = currVertex;
                                }
                            }
                            // create edge
                            log.info("init edge = {} - {}", left, right);
                            edge.add(new Edge(left, right, leftField.getName(), rightField.getName())); // maybe use field name here
                        }
                        // retrieve join fields
//                        RelDataType rowType = node.getRowType();
//                        List<RelDataTypeField> fieldList = rowType.getFieldList();
//                        RexCall condition = (RexCall) ((Join) node).getCondition();
//                        List<RexNode> operands = condition.getOperands();
//                        RexInputRef leftIndex = (RexInputRef) operands.get(0);
//                        RexInputRef rightIndex = (RexInputRef) operands.get(1);
//                        RelDataTypeField leftField = fieldList.get(leftIndex.getIndex());
//                        RelDataTypeField rightField = fieldList.get(rightIndex.getIndex());
                    }
                    super.visit(node, ordinal, parent);
                }
            };
            visitor.go(root);
        }

    }

    @AllArgsConstructor
    static class Action {

        Vertex left;
        Vertex right;
        Edge edge;
        Convention convention;

        INDArray toVector() {
            // left vector + right vector + convention vector
            return Nd4j.concat(1, IOUtil.relNode2Vector(left.node),
                    IOUtil.relNode2Vector(right.node),
                    IOUtil.convention2Vector(convention));
        }

        Vertex toNewVertex() {
//            // if this is leaf node, first implement it
//            if (left.node.getConvention().equals(Convention.NONE)){
//                Jdbc
//            }
            // add join node
            RelBuilderFactory relBuilderFactory = RelFactories.LOGICAL_BUILDER;
            final RelBuilder relBuilder = relBuilderFactory.create(left.node.getCluster(), null);
//            RelBuilderFactory jdbcBuilder = JdbcRules.JDBC_BUILDER;
//            RelBuilder relBuilder = jdbcBuilder.create(left.node.getCluster(), null); // no need schema for scan node already exist
            RelNode newNode = relBuilder
                    .push(left.node)
                    .push(right.node)
                    .joinWithLRField(JoinRelType.INNER, edge.lFieldName, edge.rFieldName)
                    .build();
            // do volcano planner to find the physical plan (appoint the converter rules)
            List<RelOptRule> rules = Lists.newArrayList();
            rules.addAll(JdbcRules.rules((JdbcConvention) left.convention));
            rules.addAll(JdbcRules.rules((JdbcConvention) right.convention));
            if (!left.convention.equals(convention)) {
                rules.add(new JdbcToJdbcConverterRule((JdbcConvention) left.convention, (JdbcConvention) convention, relBuilderFactory));
            } else if (!right.convention.equals(convention)) {
                rules.add(new JdbcToJdbcConverterRule((JdbcConvention) right.convention, (JdbcConvention) convention, relBuilderFactory));
            }
            Program program = Programs.ofRules(rules);
            RelOptPlanner planner = left.node.getCluster().getPlanner();
            RelTraitSet traitSet = planner.emptyTraitSet().replace(convention);
            RelNode optimized = program.run(planner, newNode, traitSet, Lists.newArrayList(), Lists.newArrayList());
            return Vertex.of(optimized);
        }

    }

    @AllArgsConstructor
    static class Edge {
        Vertex left;
        Vertex right;
        String lFieldName;
        String rFieldName;
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

//        @Override
//        public boolean equals(Object o) {
//            if (this == o) return true;
//            if (o == null || getClass() != o.getClass()) return false;
//            Vertex vertex = (Vertex) o;
//            return Objects.equals(node, vertex.node) &&
//                    Objects.equals(convention, vertex.convention);
//        }
//
//        @Override
//        public int hashCode() {
//            return Objects.hash(node, convention);
//        }
    }

}

interface IMDP {

    EpisodeSample.Graph getGraph();

    List<EpisodeSample.Action> getActionSpace();

    boolean isDone();

    void takeAction(EpisodeSample.Action action);
}

interface ITransformer {

    RelNode mdp2RelNode();

    void mdp2DataFile();

}
