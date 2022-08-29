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

package org.apache.druid.queryng.operator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Queries;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.RetryQueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.context.ResponseContext.Keys;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.ConcatOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.OrderedMergeOperator;
import org.apache.druid.queryng.operators.OrderedMergeOperator.Input;
import org.apache.druid.segment.SegmentMissingException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Retries the scatter phase based on missing segments. Designed to mimic
 * the {@link RetryQueryRunner} behavior. Executes a series of input operators,
 * each of which performs a scatter/gather phase. Each phase merges its results.
 * After each phase, checks if any missing segments were reported. If so,
 * launches another scatter/gather phase for just those missing segments. Repeats
 * until there are no more missing segments, or until we reach a limit.
 * <p>
 * The result is a list of input operators, each with the results for a
 * single phase. Then uses an ordered merge operator to combine the multiple
 * phases. An ordered merge means we can't start reading from any of the inputs
 * until it has seen all of them. The merge itself is delegated to the
 * {@link OrderedMergeOperator}: this operator creates the {@code Input} objects
 * which the ordered merge requires. Those inputs cause the sub-DAG to read the
 * first row, which forces the query to start running and report its missing
 * fragments.
 * <p>
 * Short-circuits the merge if the first phase had no missing segments.
 * In this case, we simply return the "base" operator as our output.
 *
 * @see {@link RetryQueryRunner}
 */
public class RetryOperator<T> implements Operator<T>
{
  private static final Logger LOG = new Logger(RetryOperator.class);

  private final QueryPlus<T> queryPlus;
  private final FragmentContext context;
  private final Operator<T> baseOperator;
  private final List<Input<T>> inputs = new ArrayList<>();
  private final BiFunction<Query<T>, List<SegmentDescriptor>, QueryRunner<T>> retryRunnerCreateFn;
  private final BiFunction<String, ResponseContext, List<SegmentDescriptor>> missingSegmentFn;
  private final int maxRetries;
  private final boolean allowPartialResults;
  private Operator<T> mergeOp;
  private State state = State.START;

  /**
   * Runnable executed after the broker creates the query distribution tree
   * for the first attempt. This is only for testing and must not be used
   * in production code.
   */
  private final Runnable runnableAfterFirstAttempt;

  public RetryOperator(
      final FragmentContext context,
      final QueryPlus<T> queryPlus,
      final Operator<T> baseOperator,
      final BiFunction<Query<T>, List<SegmentDescriptor>, QueryRunner<T>> retryRunnerCreateFn,
      final BiFunction<String, ResponseContext, List<SegmentDescriptor>> missingSegmentFn,
      final int maxRetries,
      final boolean allowPartialResults,
      final Runnable runnableAfterFirstAttempt
  )
  {
    this.context = context;
    this.queryPlus = queryPlus;
    this.baseOperator = baseOperator;
    this.retryRunnerCreateFn = retryRunnerCreateFn;
    this.missingSegmentFn = missingSegmentFn;
    this.maxRetries = QueryContexts.getNumRetriesOnMissingSegments(
        queryPlus.getQuery(),
        maxRetries
        );
    this.allowPartialResults = QueryContexts.allowReturnPartialResults(
        queryPlus.getQuery(),
        allowPartialResults);
    this.runnableAfterFirstAttempt = runnableAfterFirstAttempt;
  }

  @Override
  public ResultIterator<T> open()
  {
    Operator<T> inputOp = baseOperator;
    for (int retryCount = 0; inputOp != null; retryCount++) {
      inputOp = launchRound(retryCount, inputOp);
    }
    mergeOp = chooseMerge();
    state = State.RUN;
    return mergeOp.open();
  }

  private Operator<T> chooseMerge()
  {
    // If no retries, then return the one and only input. However, we've
    // already fetched a row, so we need a push back operator to "unread"
    // that row.
    // TODO: Would be better to ensure that we did the actual distribution
    // in open (not the first read) so we don't need the push-back trick.
    if (inputs.size() == 1) {
      return inputs.get(0).toOperator(context);
    }

    // If the query uses the natural ordering, then assume this means
    // unordered and do a concat, which is cheaper than an ordered merge.
    Ordering<T> ordering = queryPlus.getQuery().getResultOrdering();
    if (ordering == Ordering.natural()) {
      List<Operator<T>> inputOps = new ArrayList<>();
      for (Input<T> input : inputs) {
        inputOps.add(input.toOperator(context));
      }
      return new ConcatOperator<T>(context, inputOps);
    }

    // Do the fancy, ordered merge.
    return new OrderedMergeOperator<T>(
        context,
        ordering,
        inputs.size(),
        () -> inputs);
  }

  private Operator<T> launchRound(int retryCount, Operator<T> inputOp)
  {
    // Create a merge input. Doing so runs the base operator and fetches
    // the first row. That causes the missing segments to be available.
    inputs.add(new Input<T>(inputOp));
    if (retryCount == 0) {
      // runnableAfterFirstAttempt is only for testing, it must be no-op for production code.
      runnableAfterFirstAttempt.run();
    }

    // Any missing segments?
    List<SegmentDescriptor> missingSegments = missingSegmentFn.apply(
        queryPlus.getQuery().getMostSpecificId(),
        context.responseContext());

    if (missingSegments.isEmpty()) {
      return null;
    }

    // Too many retries?
    if (retryCount >= maxRetries) {
      if (!allowPartialResults) {
        throw new SegmentMissingException("No results found for segments [%s]", missingSegments);
      } else {
        return null;
      }
    }

    // Retry
    LOG.info("[%,d] missing segments found. Retry attempt [%,d]", missingSegments.size(), retryCount);

    ResponseContext context = this.context.responseContext();
    context.initializeMissingSegments();
    final QueryPlus<T> retryQueryPlus = queryPlus.withQuery(
        Queries.withSpecificSegments(queryPlus.getQuery(), missingSegments)
    );
    Sequence<T> sequence = retryRunnerCreateFn.apply(retryQueryPlus.getQuery(), missingSegments).run(retryQueryPlus, context);
    return Operators.toOperator(this.context, sequence);
  }

  @Override
  public void close(boolean cascade)
  {
    if (state == State.RUN && cascade) {
      if (mergeOp != null) {
        mergeOp.close(cascade);
      } else {
        for (Input<T> input : inputs) {
          input.close(cascade);
        }
      }
    }
    state = State.CLOSED;
  }

  public static List<SegmentDescriptor> getMissingSegments(
      String mostSpecificId,
      final ResponseContext context,
      ObjectMapper jsonMapper)
  {
    // Sanity check before retrieving missing segments from the response context.
    // The missingSegments in the responseContext is only valid when all servers have responded to the broker.
    // The remainingResponses MUST be not null but 0 in the response context at this point.
    final ConcurrentHashMap<String, Integer> idToRemainingResponses =
        Preconditions.checkNotNull(
            context.getRemainingResponses(),
            "%s in responseContext",
            Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS.getName()
        );

    final int remainingResponses = Preconditions.checkNotNull(
        idToRemainingResponses.get(mostSpecificId),
        "Number of remaining responses for query [%s]",
        mostSpecificId
    );
    if (remainingResponses > 0) {
      throw new ISE("Failed to check missing segments due to missing responses from [%d] servers", remainingResponses);
    }

    // TODO: the sender's response may contain a truncated list of missing segments.
    // Truncation is aggregated in the response context given as a parameter.
    // Check the getTruncated() value: if true, then the we don't know the full set of
    // missing segments.
    final List<SegmentDescriptor> maybeMissingSegments = context.getMissingSegments();
    if (maybeMissingSegments == null) {
      return Collections.emptyList();
    }

    return jsonMapper.convertValue(
        maybeMissingSegments,
        new TypeReference<List<SegmentDescriptor>>()
        {
        }
    );
  }
}
