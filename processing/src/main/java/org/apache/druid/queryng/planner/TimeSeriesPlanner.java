/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.queryng.planner;

import com.google.common.collect.ImmutableList;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryWatcher;
import org.apache.druid.query.Result;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.LimitOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.general.ScatterGatherOperator.OrderedScatterGatherOperator;
import org.apache.druid.queryng.operators.timeseries.GrandTotalOperator;
import org.apache.druid.queryng.operators.timeseries.IntermediateAggOperator;

import java.util.Comparator;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Time series-specific parts of the hybrid query planner.
 */
public class TimeSeriesPlanner
{
  private static class MergeShimQueryRunner implements QueryRunner<Result<TimeseriesResultValue>>
  {
    private QueryRunner<Result<TimeseriesResultValue>> inputQueryRunner;
    private Function<Query<Result<TimeseriesResultValue>>, Comparator<Result<TimeseriesResultValue>>> comparatorGenerator;
    private Function<Query<Result<TimeseriesResultValue>>, BinaryOperator<Result<TimeseriesResultValue>>> mergeFnGenerator;

    private MergeShimQueryRunner(
        QueryRunner<Result<TimeseriesResultValue>> inputQueryRunner,
        Function<Query<Result<TimeseriesResultValue>>, Comparator<Result<TimeseriesResultValue>>> comparatorGenerator,
        Function<Query<Result<TimeseriesResultValue>>, BinaryOperator<Result<TimeseriesResultValue>>> mergeFnGenerator
    )
    {
      this.inputQueryRunner = inputQueryRunner;
      this.comparatorGenerator = comparatorGenerator;
      this.mergeFnGenerator = mergeFnGenerator;
    }

    @Override
    public Sequence<Result<TimeseriesResultValue>> run(QueryPlus<Result<TimeseriesResultValue>> queryPlus,
        ResponseContext responseContext)
    {
      return mergeResults(
          queryPlus,
          responseContext,
          inputQueryRunner,
          comparatorGenerator,
          mergeFnGenerator
      );
    }
  }

  public QueryRunner<Result<TimeseriesResultValue>> mergeResultsShim(
      QueryRunner<Result<TimeseriesResultValue>> inputQueryRunner,
      Function<Query<Result<TimeseriesResultValue>>, Comparator<Result<TimeseriesResultValue>>> comparatorGenerator,
      Function<Query<Result<TimeseriesResultValue>>, BinaryOperator<Result<TimeseriesResultValue>>> mergeFnGenerator
  )
  {
    return new MergeShimQueryRunner(
        inputQueryRunner,
        comparatorGenerator,
        mergeFnGenerator
    );
  }

  /**
   * Plan a time-series grand merge: combine per-segment values to create
   * totals (if requested), limit (if requested), and create grand totals
   * (if requested).
   *
   * @see TimeseriesQueryQueryToolChest#mergeResults
   */
  public static Sequence<Result<TimeseriesResultValue>> mergeResults(
      QueryPlus<Result<TimeseriesResultValue>> queryPlus,
      ResponseContext responseContext,
      QueryRunner<Result<TimeseriesResultValue>> inputQueryRunner,
      Function<Query<Result<TimeseriesResultValue>>, Comparator<Result<TimeseriesResultValue>>> comparatorGenerator,
      Function<Query<Result<TimeseriesResultValue>>, BinaryOperator<Result<TimeseriesResultValue>>> mergeFnGenerator
  )
  {
    FragmentContext fragmentContext = queryPlus.fragment();
    final TimeseriesQuery query = (TimeseriesQuery) queryPlus.getQuery();
    Operator<Result<TimeseriesResultValue>> inputOp = Operators.toOperator(inputQueryRunner, queryPlus);

    Operator<Result<TimeseriesResultValue>> op;
    if (QueryContexts.isBySegment(queryPlus.getQuery())) {
      op = inputOp;
    } else {
      final Supplier<Result<TimeseriesResultValue>> zeroResultSupplier;
      // When granularity = ALL, there is no grouping key for this query.
      // To be more sql-compliant, we should return something (e.g., 0 for count queries) even when
      // the sequence is empty.
      if (query.getGranularity().equals(Granularities.ALL) &&
              // Returns empty sequence if this query allows skipping empty buckets
              !query.isSkipEmptyBuckets() &&
              // Returns empty sequence if bySegment is set because bySegment results are mostly used for
              // caching in historicals or debugging where the exact results are preferred.
              !QueryContexts.isBySegment(query)) {
        // A bit of a hack to avoid passing the query into the operator.
        zeroResultSupplier = () -> TimeseriesQueryQueryToolChest.getNullTimeseriesResultValue(query);
      } else {
        zeroResultSupplier = null;
      }
      // Don't do post aggs until
      // TimeseriesQueryQueryToolChest.makePostComputeManipulatorFn() is called
      TimeseriesQuery aggQuery = query.withPostAggregatorSpecs(ImmutableList.of());
      op = new IntermediateAggOperator(
          fragmentContext,
          inputOp,
          comparatorGenerator.apply(aggQuery),
          mergeFnGenerator.apply(aggQuery),
          zeroResultSupplier
      );
    }

    // Apply limit to the aggregated values.
    int limit = query.getLimit();
    if (limit < Integer.MAX_VALUE) {
      op = new LimitOperator<Result<TimeseriesResultValue>>(fragmentContext, op, limit);
    }

    if (query.isGrandTotal()) {
      op = new GrandTotalOperator(
          fragmentContext,
          op,
          query.getAggregatorSpecs()
      );
    }

    // Return the result as a sequence.
    return Operators.toSequence(op);
  }

  public static <T> Sequence<T> scatterGather(
      final QueryPlus<T> queryPlus,
      final QueryProcessingPool queryProcessingPool,
      final Iterable<QueryRunner<T>> queryables,
      final QueryWatcher queryWatcher
  )
  {
    Operator<T> op = new OrderedScatterGatherOperator<T>(
        queryPlus,
        queryProcessingPool,
        queryables,
        queryPlus.getQuery().getResultOrdering(),
        queryWatcher
    );
    return Operators.toSequence(op);
  }
}
