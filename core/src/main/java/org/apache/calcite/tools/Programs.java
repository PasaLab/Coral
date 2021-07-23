/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.tools;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.jdbc.util.GlobalInfo;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.plan.*;
import org.apache.calcite.plan.dqn.CostModel;
import org.apache.calcite.plan.dqn.OptimizerMLP;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.plan.musqle.MuSQLEPlanner;
import org.apache.calcite.plan.rf.RFPlanner;
import org.apache.calcite.plan.sloth.SlothPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.ChainedRelMetadataProvider;
import org.apache.calcite.rel.metadata.DefaultRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.rules.*;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.sql2rel.SqlToRelConverter;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utilities for creating {@link Program}s.
 */
public class Programs {
  @Deprecated // to be removed before 2.0
  public static final ImmutableList<RelOptRule> CALC_RULES = RelOptRules.CALC_RULES;

  /** Program that converts filters and projects to {@link Calc}s. */
  public static final Program CALC_PROGRAM =
      calc(DefaultRelMetadataProvider.INSTANCE);

  /** Program that expands sub-queries. */
  public static final Program SUB_QUERY_PROGRAM =
      subQuery(DefaultRelMetadataProvider.INSTANCE);

  public static final ImmutableSet<RelOptRule> DQN_PRE_RULE_SET = ImmutableSet.of(
          FilterMergeRule.INSTANCE,
          ProjectMergeRule.INSTANCE,
          ProjectJoinTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          FilterProjectTransposeRule.INSTANCE
  );

  public static final ImmutableSet<RelOptRule> HEURISTIC_POST_RULE_SET = ImmutableSet.of(
          ProjectMergeRule.INSTANCE,
          ProjectJoinTransposeRule.INSTANCE
  );

  public static final ImmutableSet<RelOptRule> RULE_SET =
      ImmutableSet.of(
          EnumerableRules.ENUMERABLE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_MERGE_JOIN_RULE,
          EnumerableRules.ENUMERABLE_CORRELATE_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_RULE,
          EnumerableRules.ENUMERABLE_FILTER_RULE,
          EnumerableRules.ENUMERABLE_AGGREGATE_RULE,
          EnumerableRules.ENUMERABLE_SORT_RULE,
          EnumerableRules.ENUMERABLE_LIMIT_RULE,
          EnumerableRules.ENUMERABLE_UNION_RULE,
          EnumerableRules.ENUMERABLE_INTERSECT_RULE,
          EnumerableRules.ENUMERABLE_MINUS_RULE,
          EnumerableRules.ENUMERABLE_TABLE_MODIFICATION_RULE,
          EnumerableRules.ENUMERABLE_VALUES_RULE,
          EnumerableRules.ENUMERABLE_WINDOW_RULE,
          EnumerableRules.ENUMERABLE_MATCH_RULE,
          SemiJoinRule.PROJECT,
          SemiJoinRule.JOIN,
          TableScanRule.INSTANCE,
          MatchRule.INSTANCE,
          CalciteSystemProperty.COMMUTE.value()
              ? JoinAssociateRule.INSTANCE
              : ProjectMergeRule.INSTANCE,
          AggregateStarTableRule.INSTANCE,
          AggregateStarTableRule.INSTANCE2,
          FilterTableScanRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterJoinRule.FILTER_ON_JOIN,
          AggregateExpandDistinctAggregatesRule.INSTANCE,
          AggregateReduceFunctionsRule.INSTANCE,
          FilterAggregateTransposeRule.INSTANCE,
          JoinCommuteRule.INSTANCE,
          JoinPushThroughJoinRule.RIGHT,
          JoinPushThroughJoinRule.LEFT,
          SortProjectTransposeRule.INSTANCE);

  // private constructor for utility class
  private Programs() {}

  /** Creates a program that executes a rule set. */
  public static Program of(RuleSet ruleSet) {
    return new RuleSetProgram(ruleSet);
  }

  /** Creates a list of programs based on an array of rule sets. */
  public static List<Program> listOf(RuleSet... ruleSets) {
    return Lists.transform(Arrays.asList(ruleSets), Programs::of);
  }

  /** Creates a list of programs based on a list of rule sets. */
  public static List<Program> listOf(List<RuleSet> ruleSets) {
    return Lists.transform(ruleSets, Programs::of);
  }

  /** Creates a program from a list of rules. */
  public static Program ofRules(RelOptRule... rules) {
    return of(RuleSets.ofList(rules));
  }

  /** Creates a program from a list of rules. */
  public static Program ofRules(Iterable<? extends RelOptRule> rules) {
    return of(RuleSets.ofList(rules));
  }

  /** Creates a program that executes a sequence of programs. */
  public static Program sequence(Program... programs) {
    return new SequenceProgram(ImmutableList.copyOf(programs));
  }

  /** Creates a program that executes a list of rules in a HEP planner. */
  public static Program hep(Iterable<? extends RelOptRule> rules,
      boolean noDag, RelMetadataProvider metadataProvider) {
    final HepProgramBuilder builder = HepProgram.builder();
    for (RelOptRule rule : rules) {
      builder.addRuleInstance(rule);
    }
    return of(builder.build(), noDag, metadataProvider);
  }

  /** Creates a program that executes a {@link HepProgram}. */
  public static Program of(final HepProgram hepProgram, final boolean noDag,
      final RelMetadataProvider metadataProvider) {
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      final HepPlanner hepPlanner = new HepPlanner(hepProgram,
          null, noDag, null, RelOptCostImpl.FACTORY);

      List<RelMetadataProvider> list = new ArrayList<>();
      if (metadataProvider != null) {
        list.add(metadataProvider);
      }
      hepPlanner.registerMetadataProviders(list);
      for (RelOptMaterialization materialization : materializations) {
        hepPlanner.addMaterialization(materialization);
      }
      for (RelOptLattice lattice : lattices) {
        hepPlanner.addLattice(lattice);
      }
      RelMetadataProvider plannerChain =
          ChainedRelMetadataProvider.of(list);
      rel.getCluster().setMetadataProvider(plannerChain);

      hepPlanner.setRoot(rel);
      return hepPlanner.findBestExp();
    };
  }

  public static Program calc(RelMetadataProvider metadataProvider) {
    return hep(RelOptRules.CALC_RULES, true, metadataProvider);
  }

  @Deprecated // to be removed before 2.0
  public static Program subquery(RelMetadataProvider metadataProvider) {
    return subQuery(metadataProvider);
  }

  public static Program subQuery(RelMetadataProvider metadataProvider) {
    final HepProgramBuilder builder = HepProgram.builder();
    builder.addRuleCollection(ImmutableList.of((RelOptRule) SubQueryRemoveRule.FILTER,
            SubQueryRemoveRule.PROJECT,
            SubQueryRemoveRule.JOIN));
    return of(builder.build(), true, metadataProvider);
  }


  /** Creates a program that invokes heuristic join-order optimization
   * (via {@link org.apache.calcite.rel.rules.JoinToMultiJoinRule},
   * {@link org.apache.calcite.rel.rules.MultiJoin} and
   * {@link org.apache.calcite.rel.rules.LoptOptimizeJoinRule})
   * if there are 6 or more joins (7 or more relations). */
  public static Program heuristicJoinOrder(
      final Iterable<? extends RelOptRule> rules,
      final boolean bushy, final int minJoinCount) {
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      final int joinCount = RelOptUtil.countJoins(rel);
      final Program program;
      if (joinCount < minJoinCount) {
        program = ofRules(rules);
      } else {
        // Create a program that gathers together joins as a MultiJoin.
        final HepProgram hep = new HepProgramBuilder()
            .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
            .addMatchOrder(HepMatchOrder.BOTTOM_UP)
            .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
            .build();
        final Program program1 =
            of(hep, false, DefaultRelMetadataProvider.INSTANCE);

        // Create a program that contains a rule to expand a MultiJoin
        // into heuristically ordered joins.
        // We use the rule set passed in, but remove JoinCommuteRule and
        // JoinPushThroughJoinRule, because they cause exhaustive search.
        final List<RelOptRule> list = Lists.newArrayList(rules);
        list.removeAll(
            ImmutableList.of(JoinCommuteRule.INSTANCE,
                JoinAssociateRule.INSTANCE,
                JoinPushThroughJoinRule.LEFT,
                JoinPushThroughJoinRule.RIGHT));
        list.add(bushy
            ? MultiJoinOptimizeBushyRule.INSTANCE
            : LoptOptimizeJoinRule.INSTANCE);
        final Program program2 = ofRules(list);

        program = sequence(program1, program2);
      }
      return program.run(
          planner, rel, requiredOutputTraits, materializations, lattices);
    };
  }

  public static Program heuristicJoinOrder(final boolean bushy) {
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      final Program program;
      // Create a program that gathers together joins as a MultiJoin.
      final HepProgram hep = new HepProgramBuilder()
              .addRuleInstance(FilterJoinRule.FILTER_ON_JOIN)
              .addMatchOrder(HepMatchOrder.BOTTOM_UP)
              .addRuleInstance(JoinToMultiJoinRule.INSTANCE)
              .build();
      final Program program1 =
              of(hep, true, DefaultRelMetadataProvider.INSTANCE);

      // Create a program that contains a rule to expand a MultiJoin
      // into heuristically ordered joins.
      // We use the rule set passed in, but remove JoinCommuteRule and
      // JoinPushThroughJoinRule, because they cause exhaustive search.
      final List<RelOptRule> list = Lists.newArrayList(planner.getRules());
      list.removeAll(
              ImmutableList.of(JoinCommuteRule.INSTANCE,
                      JoinAssociateRule.INSTANCE,
                      JoinPushThroughJoinRule.LEFT,
                      JoinPushThroughJoinRule.RIGHT));
      list.add(bushy
              ? MultiJoinOptimizeBushyRule.INSTANCE
              : LoptOptimizeJoinRule.INSTANCE);
      final Program program2 = ofRules(list);

      program = sequence(
              new TrimFieldsProgram(),
              program1,
              program2);

      return program.run(
              planner, rel, requiredOutputTraits, materializations, lattices);
    };
  }

  public static Program fixedPlatform() {
    Program program1 = (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      final List<RelOptRule> list = Lists.newArrayList(planner.getRules());
      list.removeAll(
              ImmutableList.of(JoinCommuteRule.INSTANCE,
                      JoinAssociateRule.INSTANCE,
                      JoinPushThroughJoinRule.LEFT,
                      JoinPushThroughJoinRule.RIGHT));
      final Program program = ofRules(list);
      return program.run(
              planner, rel, requiredOutputTraits, materializations, lattices);
    };
    return sequence(subQuery(DefaultRelMetadataProvider.INSTANCE),
            new DecorrelateProgram(),
            new TrimFieldsProgram(),
            program1,
            calc(DefaultRelMetadataProvider.INSTANCE));
  }

  public static Program fixedOptimizer(){
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      OptimizerKind optimizerKind = GlobalInfo.getInstance().getFixedOptimizer();
      RelMetadataProvider metadataProvider = DefaultRelMetadataProvider.INSTANCE;
      Program specifyProgram;
      switch (optimizerKind){
        case SLOTH:
//          specifyProgram = new SlothProgram();
//          break;
            return standard().run(planner, rel, requiredOutputTraits, materializations, lattices);
        case MUSQLE:
          specifyProgram = new MuSQLEProgram();
          break;
        case RF:
          specifyProgram = new RFProgram();
          break;
        default:
          return standard().run(planner, rel, requiredOutputTraits, materializations, lattices);
      }
      Program program = sequence(
              subQuery(metadataProvider),
              new DecorrelateProgram(),
              new TrimFieldsProgram(),
              dqnPreWork(metadataProvider),
              specifyProgram,
              calc(metadataProvider));
      return program.run(
              planner, rel, requiredOutputTraits, materializations, lattices);
    };
  }

  /** Returns the standard program used by Prepare. */
  public static Program standard() {
    return standard(DefaultRelMetadataProvider.INSTANCE);
  }

  /** Returns the standard program with user metadata provider. */
  public static Program standard(RelMetadataProvider metadataProvider) {
    final Program program1 =
        (planner, rel, requiredOutputTraits, materializations, lattices) -> {
          // 将relnode注册到优化器，优化器会将relnode转换为优化器定义的数据结构
          // 注册时遍历树的每个结点，进行规则匹配，将匹配的规则存储下来
          planner.setRoot(rel);

          for (RelOptMaterialization materialization : materializations) {
            planner.addMaterialization(materialization);
          }
          for (RelOptLattice lattice : lattices) {
            planner.addLattice(lattice);
          }

          final RelNode rootRel2 =
              rel.getTraitSet().equals(requiredOutputTraits)
                  ? rel
                  : planner.changeTraits(rel, requiredOutputTraits);
          assert rootRel2 != null;

          planner.setRoot(rootRel2);
          final RelOptPlanner planner2 = planner.chooseDelegate();
          // 找到最好的规则，将其应用得到最优的表达式，重复这个过程
          final RelNode rootRel3 = planner2.findBestExp();
          assert rootRel3 != null : "could not implement exp";
          return rootRel3;
        };

    return sequence(subQuery(metadataProvider),
        new DecorrelateProgram(),
        new TrimFieldsProgram(),
        program1,

        // Second planner pass to do physical "tweaks". This the first time
        // that EnumerableCalcRel is introduced.
        calc(metadataProvider));
  }

  /**
   * create Program that
   * program. push down predicate and project
   */
  public static Program dqnPreWork(RelMetadataProvider metadataProvider){
    return (planner, rel, requiredOutputTraits, materializations, lattices) -> {
      final HepProgramBuilder builder = HepProgram.builder();
      for (RelOptRule rule : DQN_PRE_RULE_SET) {
        builder.addRuleInstance(rule);
      }
      final Program program =
              of(builder.build(), true, metadataProvider);
      return program.run(planner, rel, requiredOutputTraits, materializations, lattices);
    };
  }

  /** Returns the dqn program used by Prepare. */
  public static Program dqn() {
    return dqn(DefaultRelMetadataProvider.INSTANCE);
  }

  /** Returns the standard program with user metadata provider. */
  public static Program dqn(RelMetadataProvider metadataProvider) {
    // todo [coral] gather jdbc rules
    return sequence(subQuery(metadataProvider),
            new DecorrelateProgram(),  // 去关联
            new TrimFieldsProgram(),  // 删除无用属性
            // trivial predicate push down && implement
            dqnPreWork(metadataProvider), // 使用简单的规则进行优化
            // join reorder && platform choice
            new DQNProgram(),
            // Second planner pass to do physical "tweaks". This the first time
            // that EnumerableCalcRel is introduced.
            // 使用另一个优化器进行物理调整
            calc(metadataProvider));
  }

  /** Program backed by a {@link RuleSet}. */
  static class RuleSetProgram implements Program {
    final RuleSet ruleSet;

    private RuleSetProgram(RuleSet ruleSet) {
      this.ruleSet = ruleSet;
    }

    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices) {
      planner.clear();
      for (RelOptRule rule : ruleSet) {
        planner.addRule(rule);
      }
      for (RelOptMaterialization materialization : materializations) {
        planner.addMaterialization(materialization);
      }
      for (RelOptLattice lattice : lattices) {
        planner.addLattice(lattice);
      }
      if (!rel.getTraitSet().equals(requiredOutputTraits)) {
        rel = planner.changeTraits(rel, requiredOutputTraits);
      }
      planner.setRoot(rel);
      return planner.findBestExp();

    }
  }

  /** Program that runs sub-programs, sending the output of the previous as
   * input to the next. */
  private static class SequenceProgram implements Program {
    private final ImmutableList<Program> programs;

    SequenceProgram(ImmutableList<Program> programs) {
      this.programs = programs;
    }

    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices) {
      for (Program program : programs) {
        rel = program.run(
            planner, rel, requiredOutputTraits, materializations, lattices);
      }
      return rel;
    }
  }

  /** Program that de-correlates a query.
   *
   * <p>To work around
   * <a href="https://issues.apache.org/jira/browse/CALCITE-842">[CALCITE-842]
   * Decorrelator gets field offsets confused if fields have been trimmed</a>,
   * disable field-trimming in {@link SqlToRelConverter}, and run
   * {@link TrimFieldsProgram} after this program. */
  private static class DecorrelateProgram implements Program {
    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices) {
      final CalciteConnectionConfig config =
          planner.getContext().unwrap(CalciteConnectionConfig.class);
      if (config != null && config.forceDecorrelate()) {
        final RelBuilder relBuilder =
            RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
        return RelDecorrelator.decorrelateQuery(rel, relBuilder);
      }
      return rel;
    }
  }

  /** Program that trims fields. */
  private static class TrimFieldsProgram implements Program {
    public RelNode run(RelOptPlanner planner, RelNode rel,
        RelTraitSet requiredOutputTraits,
        List<RelOptMaterialization> materializations,
        List<RelOptLattice> lattices) {
      final RelBuilder relBuilder =
          RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      return new RelFieldTrimmer(null, relBuilder).trim(rel);
    }
  }

  /**
   * Program that join reorder && join platform choice && implement plan to JDBC env
   */
  private static class DQNProgram implements Program {
    public RelNode run(RelOptPlanner planner, RelNode rel,
                       RelTraitSet requiredOutputTraits,
                       List<RelOptMaterialization> materializations,
                       List<RelOptLattice> lattices) {
      final RelBuilder relBuilder =
              RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      return OptimizerMLP.execute(rel, relBuilder, CostModel.COST_MODEL_1);
    }
  }

  /**
   * Sloth Program
   */
  private static class SlothProgram implements Program {
    public RelNode run(RelOptPlanner planner, RelNode rel,
                       RelTraitSet requiredOutputTraits,
                       List<RelOptMaterialization> materializations,
                       List<RelOptLattice> lattices) {
      final RelBuilder relBuilder =
              RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      return SlothPlanner.optimize(rel, relBuilder, requiredOutputTraits);
    }
  }

  /**
   * MuSQLE Program
   */
  private static class MuSQLEProgram implements Program {
    public RelNode run(RelOptPlanner planner, RelNode rel,
                       RelTraitSet requiredOutputTraits,
                       List<RelOptMaterialization> materializations,
                       List<RelOptLattice> lattices) {
      final RelBuilder relBuilder =
              RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      return MuSQLEPlanner.optimize(rel, relBuilder, requiredOutputTraits);
    }
  }

  /**
   * Random Forest Program
   */
  private static class RFProgram implements Program {
    public RelNode run(RelOptPlanner planner, RelNode rel,
                       RelTraitSet requiredOutputTraits,
                       List<RelOptMaterialization> materializations,
                       List<RelOptLattice> lattices) {
      final RelBuilder relBuilder =
              RelFactories.LOGICAL_BUILDER.create(rel.getCluster(), null);
      return RFPlanner.optimize(rel, relBuilder, CostModel.COST_MODEL_1);
    }
  }
}

// End Programs.java
