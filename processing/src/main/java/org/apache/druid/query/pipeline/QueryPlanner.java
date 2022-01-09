package org.apache.druid.query.pipeline;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;
import java.util.function.Supplier;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.BySegmentResultValue;
import org.apache.druid.query.BySegmentResultValueClass;
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
 */
public class QueryPlanner
{
  protected static <T> Sequence<T> toSequence(
      final Operator op,
      final Query<?> query,
      final ResponseContext responseContext)
  {
    return Operators.toSequence(
        op,
        // TODO(paul): Create this once, not for each operator.
        new FragmentContextImpl(
            query,
            responseContext));
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
    Supplier<Operator> input = Operators.toProducer(
        delegate,
        queryWithMetrics,
        responseContext);
    CpuMetricOperator op = new CpuMetricOperator(
        cpuTimeAccumulator,
        queryWithMetrics.getQueryMetrics(),
        emitter,
        input);
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
    Sequence<T> seq = baseRunner.run(queryPlus.withQuery(queryToRun), responseContext);

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
      return seq;
    } else {
      finalizerFn = baseFinalizer;
    }

    TransformOperator op = new TransformOperator(
        finalizerFn,
        Operators.toProducer(seq));
    return toSequence(
        op,
        queryToRun,
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
    Supplier<Operator> input = Operators.toProducer(
        queryRunner,
        queryWithMetrics,
        responseContext);
    MetricsOperator op = new MetricsOperator(
        emitter,
        queryMetrics,
        reportMetric,
        creationTimeNs == 0 ? null : Timer.createAt(creationTimeNs),
        input);
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
    Supplier<Operator> input = Operators.toProducer(
        factory.createRunner(segment),
        queryPlus,
        responseContext);
    SegmentLockOperator op = new SegmentLockOperator(segment, descriptor, input);
    return toSequence(
        op,
        queryPlus.getQuery(),
        responseContext);
  }
}
