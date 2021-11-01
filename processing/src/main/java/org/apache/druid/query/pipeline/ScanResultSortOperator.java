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

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.collections.StableLimitingSorter;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanQueryRunnerFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.joda.time.Interval;

/**
 * Returns a sorted and limited transformation of the input. Materializes the full sequence
 * in memory before returning it. The amount of memory use is limited by the limit of the scan query.
 * <p>
 * Simultaneously sorts and limits its input.
 * <p>
 * The sort is stable, meaning that equal elements (as determined by the comparator) will not be reordered.
 * <p>
 * Not thread-safe.
 * <p>
 * The output is a set of batches each containing a single row.
 * <p>
 * Questions:<ul>
 * <li>Are the segments sorted by date? If so, why another sort? Why not a merge?</li>
 * <li>Segments are ordered by date. Segments within a data source do not overlay.
 * So, we can sort all the partitions for one segment and return them before reading
 * any of the partitions of the next segment. Doing so will reduce the memory footprint.</li>
 * </ul>
 *
 * @see {@link ScanQueryRunnerFactory#stableLimitingSort}
 * @see {@link org.apache.druid.collections.StableLimitingSorter}
 */
public class ScanResultSortOperator implements Operator
{
  public static ScanResultSortOperator forQuery(ScanQuery query, Operator child)
  {
    // Converting the limit from long to int could theoretically throw an ArithmeticException but this branch
    // only runs if limit < MAX_LIMIT_FOR_IN_MEMORY_TIME_ORDERING (which should be < Integer.MAX_VALUE)
    final int limit;
    if (query.isLimited()) {
      if (query.getScanRowsLimit() > Integer.MAX_VALUE) {
        throw new UOE(
            "Limit of %,d rows not supported for priority queue strategy of time-ordering scan results",
            query.getScanRowsLimit()
        );
      }
      limit = Math.toIntExact(query.getScanRowsLimit());
    } else {
      limit = Integer.MAX_VALUE;
    }
    return new ScanResultSortOperator(
        limit,
        query.getResultOrdering(),
        ScanQueryRunnerFactory.getIntervalsFromSpecificQuerySpec(query.getQuerySegmentSpec()),
        query.getResultFormat(),
        child
        );
  }

  private final int limit;
  private final Comparator<ScanResultValue> comparator;
  private final List<Interval> intervalsOrdered;
  private final ResultFormat resultFormat;
  private final Operator child;
  private Iterator<ScanResultValue> resultIter;

  public ScanResultSortOperator(
      final int limit,
      final Comparator<ScanResultValue> comparator,
      final List<Interval> intervalsOrdered,
      final ResultFormat resultFormat,
      final Operator child
  )
  {
    this.limit = limit;
    this.comparator = comparator;
    this.intervalsOrdered = intervalsOrdered;
    this.resultFormat = resultFormat;
    this.child = child;
  }

  // See ScanQueryRunnerFactory.stableLimitingSort
  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Object> open(FragmentContext context) {
    StableLimitingSorter<ScanResultValue> sorter = new StableLimitingSorter<>(comparator, limit);
    // We need to scan limit elements and anything else in the last segment
    int numRowsScanned = 0;
    for (Object input : Operators.toIterable(child, context)) {
      ScanResultValue next = (ScanResultValue) input;
      Interval finalInterval = null;
      // Using an intermediate unbatched ScanResultValue is not that great memory-wise, but the column list
      // needs to be preserved for queries using the compactedList result format
      for (ScanResultValue srv : next.toSingleEventScanResultValues()) {
        numRowsScanned++;
        sorter.add(srv);

        // Finish scanning the interval containing the limit row
        if (numRowsScanned > limit && finalInterval == null) {
          long timestampOfLimitRow = srv.getFirstEventTimestamp(resultFormat);
          for (Interval interval :intervalsOrdered) {
            if (interval.contains(timestampOfLimitRow)) {
              finalInterval = interval;
            }
          }
          if (finalInterval == null) {
            throw new ISE("Row came from an unscanned interval");
          }
        }
      }
      // TODO: The format should be metadata in the batch
      if (finalInterval != null &&
          !finalInterval.contains(next.getFirstEventTimestamp(resultFormat))) {
        break;
      }
    }
    resultIter = sorter.drain();
    child.close(true);
    return (Iterator<Object>) (Iterator<?>) resultIter;
  }

//  @Override
//  public boolean hasNext() {
//    return resultIter.hasNext();
//  }
//
//  @Override
//  public Object next() {
//    return resultIter.next();
//  }

  @Override
  public void close(boolean cascade)
  {
    resultIter = null;
  }
}
