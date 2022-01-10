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

package org.apache.druid.query.pipeline;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.Result;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.aggregation.MetricManipulatorFns;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.profile.Timer;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.SegmentReference;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

/**
 * Hybrid query planner/runner to act as a "shim" between the existing
 * QueryRunner and similar abstractions and the operator-based execution
 * layer. Over time, this should be replaced with a proper low-level
 * planner that just does planning by eliminating the use of sequences.
 * Then, once the operator pipeline is planned, just run the result.
 * <p>
 * This class collects, in one place, the steps needed to plan (and, for
 * now, run) a scan query. The functions are referenced from various
 * query runners, factories, toolchests, etc. to preserve the existing
 * structure. This odd shape is an intermediate step to the goal explained
 * above.
 * <p>
 * The key thing to note is that the original runner-based model gives a
 * query as input, and returns a sequence. In this shim, the sequence is
 * a wrapper around an operator. Operators will "unwrap" the sequence of
 * we have the pattern:<br/>
 * operator &rarr; sequence &rarr operator</br
 * to produce:<br/>
 * operator &rarr; operator</br>
 * Once all runners simply produce a wrapped operator, using this class,
 * then the runners themselves, and the sequences, can melt away leaving
 * just a pipeline of operators, thereby separating the low-level plan
 * phase from the actual execution phase.
 * <p>
 * Careful inspection will show that much of the code here is redundant:
 * there is repeated wrapping and unwrapping of operators. The same
 * values are passed in repeatedly. When converted to an operator-only
 * planner, the wrapping and unwrapping will disappear, and the
 * constant-per-query values can be set in a constructor and reused.
 * <p>
 * At present, the planner consists of a bunch of static functions called
 * from query runners. As a result, all required info is passed in to
 * each function. Once we can retire the query runners, this turns into
 * a stateful class that holds the common information about a query.
 * The functions become methods that do only operator planning (without
 * the sequence wrappers.) Flow of control is between methods, not
 * from query runners into this class as in the current structure.
 */
public class QueryPlanner
{
  protected static <T> Sequence<T> toSequence(
      final Operator op,
      final Query<?> query,
      final ResponseContext responseContext)
  {
    // TODO(paul): Create context at top, so we can
    // register operators.
    // responseContext.getFragmentContext().register(op);

    return Operators.toSequence(
        op,
        // TODO(paul): Create this once, not for each operator.
        new FragmentContextImpl(
            query,
            responseContext));
  }

  protected static Operator concat(List<Operator> children) {
    return ConcatOperator.concatOrNot(children);
  }

  /**
   * @see {org.apache.druid.query.CPUTimeMetricQueryRunner}
   */
  public static <T> Sequence<T> runCpuTimeMetric(
      final QueryRunner<T> delegate,
      QueryToolChest<T, ? extends Query<T>> queryToolChest,
      final AtomicLong cpuTimeAccumulator,
      final ServiceEmitter emitter,
      final boolean report,
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext)
  {
    final QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(queryToolChest);

    // Short circuit if not reporting CPU time.
    if (!report) {
      return delegate.run(queryWithMetrics, responseContext);
    }
    Operator inputOp = Operators.toOperator(
        delegate,
        queryWithMetrics);
    CpuMetricOperator op = new CpuMetricOperator(
        cpuTimeAccumulator,
        queryWithMetrics.getQueryMetrics(),
        emitter,
        inputOp);
    return toSequence(
        op,
        queryWithMetrics.getQuery(),
        responseContext);
  }

  /**
   * Plan step that applies {@link QueryToolChest#makePostComputeManipulatorFn(Query, MetricManipulationFn)} to the
   * result stream. It is expected to be the last operator in the pipeline, after results are fully merged.
   * <p>
   * Note that despite the type parameter "T", this runner may not actually return sequences with type T. This most
   * commonly happens when an upstream {@link BySegmentQueryRunner} changes the result stream to type
   * {@code Result<BySegmentResultValue<T>>}, in which case this class will retain the structure, but call the finalizer
   * function on each result in the by-segment list (which may change their type from T to something else).
   *
   * @see {@link org.apache.druid.query.FinalizeResultsQueryRunner}
   */
  public static <T> Sequence<T> runFinalizeResults(
      final QueryRunner<T> baseRunner,
      final QueryToolChest<T, Query<T>> toolChest,
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext
  )
  {
    final Query<T> query = queryPlus.getQuery();
    final boolean shouldFinalize = QueryContexts.isFinalize(query, true);

    final Query<T> queryToRun;
    final MetricManipulationFn metricManipulationFn;

    if (shouldFinalize) {
      queryToRun = query.withOverriddenContext(ImmutableMap.of("finalize", false));
      metricManipulationFn = MetricManipulatorFns.finalizing();
    } else {
      queryToRun = query;
      metricManipulationFn = MetricManipulatorFns.identity();
    }

    final Function<T, T> baseFinalizer = toolChest.makePostComputeManipulatorFn(
        query,
        metricManipulationFn
        );
    final Function<T, ?> finalizerFn;
    if (QueryContexts.isBySegment(query)) {
      finalizerFn = new Function<T, Result<BySegmentResultValue<T>>>()
      {
        @Override
        public Result<BySegmentResultValue<T>> apply(T input)
        {
          //noinspection unchecked (input is not actually a T; see class-level javadoc)
          @SuppressWarnings("unchecked")
          Result<BySegmentResultValueClass<T>> result = (Result<BySegmentResultValueClass<T>>) input;

          if (input == null) {
            throw new ISE("Cannot have a null result!");
          }

          BySegmentResultValue<T> resultsClass = result.getValue();

          return new Result<>(
              result.getTimestamp(),
              new BySegmentResultValueClass<>(
                  Lists.transform(resultsClass.getResults(), baseFinalizer),
                  resultsClass.getSegmentId(),
                  resultsClass.getInterval()
              )
          );
        }
      };
    } else if (baseFinalizer == Functions.identity()) {
      // Optimize away the finalizer operator if nothing to do.
      return baseRunner.run(queryPlus.withQuery(queryToRun), responseContext);
    } else {
      finalizerFn = baseFinalizer;
    }

    Operator inputOp = Operators.toOperator(
        baseRunner,
        queryPlus.withQuery(queryToRun));
    TransformOperator op = new TransformOperator(
        finalizerFn,
        inputOp);
    return toSequence(
        op,
        queryToRun,
        responseContext);
  }

  /**
   * Plans for a specific segment. The {@code SpecificSegmentQueryRunner}
   * does two things: renames the thread and reports missing segments by
   * catching the {@code SegmentMissingException}. In the operator model,
   * the missing segment is caught in the
   * {@link SegmentLockOperator} and no exception is thrown.
   * <p>
   * The only place the exception is thrown is in the
   * {@code TimeseriesQueryEngine} and {@code TopNQueryEngine}. Those
   * should be caught by the corresponding operators.
   *
   * @see {@link org.apache.druid.query.spec.SpecificSegmentQueryRunner}
   */
  public static <T> Sequence<T> runSpecificSegment(
      final QueryRunner<T> base,
      final SpecificSegmentSpec specificSpec,
      final QueryPlus<T> input,
      final ResponseContext responseContext
      )
  {
    final QueryPlus<T> queryPlus = input.withQuery(
        Queries.withSpecificSegments(
            input.getQuery(),
            Collections.singletonList(specificSpec.getDescriptor())
        )
    );
    final Query<T> query = queryPlus.getQuery();
    final String newName = query.getType() + "_" + query.getDataSource() + "_" + query.getIntervals();
    Operator inputOp = Operators.toOperator(
        base,
        queryPlus);
    ThreadLabelOperator op = new ThreadLabelOperator(
        newName,
        inputOp);
    return toSequence(
        op,
        queryPlus.getQuery(),
        responseContext);
  }

  public static <T> Sequence<T> runMetrics(
      final ServiceEmitter emitter,
      final QueryToolChest<T, ? extends Query<T>> queryToolChest,
      final QueryRunner<T> queryRunner,
      final long creationTimeNs,
      final ObjLongConsumer<? super QueryMetrics<?>> reportMetric,
      final Consumer<QueryMetrics<?>> applyCustomDimensions,
      final QueryPlus<T> queryPlus,
      final ResponseContext responseContext
  )
  {
    final QueryPlus<T> queryWithMetrics = queryPlus.withQueryMetrics(queryToolChest);
    final QueryMetrics<?> queryMetrics = queryWithMetrics.getQueryMetrics();

    applyCustomDimensions.accept(queryMetrics);

    // Short circuit if no metrics.
    if (queryMetrics == null) {
      return queryRunner.run(queryWithMetrics, responseContext);
    }
    Operator inputOp = Operators.toOperator(
        queryRunner,
        queryWithMetrics);
    MetricsOperator op = new MetricsOperator(
        emitter,
        queryMetrics,
        reportMetric,
        creationTimeNs == 0 ? null : Timer.createAt(creationTimeNs),
        inputOp);
     return toSequence(
        op,
        queryWithMetrics.getQuery(),
        responseContext);
  }

  public static <T> Sequence<T> runSeqmentLock(
      final SegmentReference segment,
      final SegmentDescriptor descriptor,
      final QueryPlus<T> queryPlus,
      final QueryRunnerFactory<T, Query<T>> factory,
      ResponseContext responseContext)
  {
    Operator inputOp = Operators.toOperator(
        factory.createRunner(segment),
        queryPlus);
    SegmentLockOperator op = new SegmentLockOperator(segment, descriptor, inputOp);
    return toSequence(
        op,
        queryPlus.getQuery(),
        responseContext);
  }
}
