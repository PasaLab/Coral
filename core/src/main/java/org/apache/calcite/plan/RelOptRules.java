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
package org.apache.calcite.plan;

import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.interpreter.NoneToBindableConverterRule;
import org.apache.calcite.linq4j.function.Experimental;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.*;

import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * A utility class for organizing built-in rules and rule related
 * methods. Currently some rule sets are package private for serving core Calcite.
 *
 * @see RelOptRule
 * @see RelOptUtil
 */
@Experimental
public class RelOptRules {

  private RelOptRules() {
  }

  /**
   * The calc rule set is public for use from {@link org.apache.calcite.tools.Programs}
   */
  public static final ImmutableList<RelOptRule> CALC_RULES =
      ImmutableList.of(
          NoneToBindableConverterRule.INSTANCE,
          EnumerableRules.ENUMERABLE_CALC_RULE,
          EnumerableRules.ENUMERABLE_FILTER_TO_CALC_RULE,
          EnumerableRules.ENUMERABLE_PROJECT_TO_CALC_RULE,
          CalcMergeRule.INSTANCE,
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE,
          FilterToCalcRule.INSTANCE,
          ProjectToCalcRule.INSTANCE,
          CalcMergeRule.INSTANCE,

          // REVIEW jvs 9-Apr-2006: Do we still need these two?  Doesn't the
          // combination of CalcMergeRule, FilterToCalcRule, and
          // ProjectToCalcRule have the same effect?
          FilterCalcMergeRule.INSTANCE,
          ProjectCalcMergeRule.INSTANCE);

  static final List<RelOptRule> BASE_RULES = ImmutableList.of(
      AggregateStarTableRule.INSTANCE,
      AggregateStarTableRule.INSTANCE2,
      TableScanRule.INSTANCE,
      CalciteSystemProperty.COMMUTE.value()
          ? JoinAssociateRule.INSTANCE
          : ProjectMergeRule.INSTANCE,
      FilterTableScanRule.INSTANCE,
      ProjectFilterTransposeRule.INSTANCE,
      FilterProjectTransposeRule.INSTANCE,
      FilterJoinRule.FILTER_ON_JOIN,
      JoinPushExpressionsRule.INSTANCE,
      AggregateExpandDistinctAggregatesRule.INSTANCE,
      AggregateCaseToFilterRule.INSTANCE,
      AggregateReduceFunctionsRule.INSTANCE,
      FilterAggregateTransposeRule.INSTANCE,
      ProjectWindowTransposeRule.INSTANCE,
      MatchRule.INSTANCE,
      JoinCommuteRule.INSTANCE,
      JoinPushThroughJoinRule.RIGHT,
      JoinPushThroughJoinRule.LEFT,
      SortProjectTransposeRule.INSTANCE,
      SortJoinTransposeRule.INSTANCE,
      SortRemoveConstantKeysRule.INSTANCE,
      SortUnionTransposeRule.INSTANCE,
      ExchangeRemoveConstantKeysRule.EXCHANGE_INSTANCE,
      ExchangeRemoveConstantKeysRule.SORT_EXCHANGE_INSTANCE);

  static final List<RelOptRule> ABSTRACT_RULES = ImmutableList.of(
      AggregateProjectPullUpConstantsRule.INSTANCE2,
      UnionPullUpConstantsRule.INSTANCE,
      PruneEmptyRules.UNION_INSTANCE,
      PruneEmptyRules.INTERSECT_INSTANCE,
      PruneEmptyRules.MINUS_INSTANCE,
      PruneEmptyRules.PROJECT_INSTANCE,
      PruneEmptyRules.FILTER_INSTANCE,
      PruneEmptyRules.SORT_INSTANCE,
      PruneEmptyRules.AGGREGATE_INSTANCE,
      PruneEmptyRules.JOIN_LEFT_INSTANCE,
      PruneEmptyRules.JOIN_RIGHT_INSTANCE,
      PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
      UnionMergeRule.INSTANCE,
      UnionMergeRule.INTERSECT_INSTANCE,
      UnionMergeRule.MINUS_INSTANCE,
      ProjectToWindowRule.PROJECT,
      FilterMergeRule.INSTANCE,
      DateRangeRules.FILTER_INSTANCE,
      IntersectToDistinctRule.INSTANCE);

  static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES = ImmutableList.of(
      FilterJoinRule.FILTER_ON_JOIN,
      FilterJoinRule.JOIN,
      AbstractConverter.ExpandConversionRule.INSTANCE,
      JoinCommuteRule.INSTANCE,
      SemiJoinRule.PROJECT,
      SemiJoinRule.JOIN,
      AggregateRemoveRule.INSTANCE,
      UnionToDistinctRule.INSTANCE,
      ProjectRemoveRule.INSTANCE,
      AggregateJoinTransposeRule.INSTANCE,
      AggregateMergeRule.INSTANCE,
      AggregateProjectMergeRule.INSTANCE,
      CalcRemoveRule.INSTANCE,
      SortRemoveRule.INSTANCE);

  static final List<RelOptRule> CONSTANT_REDUCTION_RULES = ImmutableList.of(
      ReduceExpressionsRule.PROJECT_INSTANCE,
      ReduceExpressionsRule.FILTER_INSTANCE,
      ReduceExpressionsRule.CALC_INSTANCE,
      ReduceExpressionsRule.WINDOW_INSTANCE,
      ReduceExpressionsRule.JOIN_INSTANCE,
      ValuesReduceRule.FILTER_INSTANCE,
      ValuesReduceRule.PROJECT_FILTER_INSTANCE,
      ValuesReduceRule.PROJECT_INSTANCE,
      AggregateValuesRule.INSTANCE);

  static final List<RelOptRule> MATERIALIZATION_RULES = ImmutableList.of(
      MaterializedViewFilterScanRule.INSTANCE,
      AbstractMaterializedViewRule.INSTANCE_PROJECT_FILTER,
      AbstractMaterializedViewRule.INSTANCE_FILTER,
      AbstractMaterializedViewRule.INSTANCE_PROJECT_JOIN,
      AbstractMaterializedViewRule.INSTANCE_JOIN,
      AbstractMaterializedViewRule.INSTANCE_PROJECT_AGGREGATE,
      AbstractMaterializedViewRule.INSTANCE_AGGREGATE);

  /**
   * The calc rule set is public for use from {@link org.apache.calcite.tools.Programs}
   */
  public static final List<RelOptRule> CORAL_RULES = ImmutableList.of(
//          SemiJoinRule.PROJECT,
//          SemiJoinRule.JOIN,
//          ProjectRemoveRule.INSTANCE,
//          ProjectMergeRule.INSTANCE,
          FilterMergeRule.INSTANCE,
//          ProjectFilterTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          ProjectJoinTransposeRule.INSTANCE,
//          FilterJoinRule.JOIN,
          FilterJoinRule.FILTER_ON_JOIN,
//          JoinCommuteRule.INSTANCE,  // 这个是会导致优化器优化速度减慢的规则，而且会带来问题
          JoinAssociateRule.INSTANCE
//          JoinPushThroughJoinRule.RIGHT, // may not useful in out test
//          JoinPushThroughJoinRule.LEFT
  );
}

// End RelOptRules.java
