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

package org.apache.druid.query.scan;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryTimeoutException;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.profile.InnerProfileStack;
import org.apache.druid.query.profile.QueryMetricsAdapter;
import org.apache.druid.query.profile.SegmentScanProfile;
import org.apache.druid.query.profile.Timer;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * The scan query engine implements the native ScanQuery by creating a Sequence which
 * will iterate over either the segment, or a set of one or more indexes on top of a
 * segment. The scan query provides per-row filtering. Some filtering is converted to
 * an indexed lookup (such as foo="bar"), and others are applied per row.
 */
public class ScanQueryEngine
{
  static final String LEGACY_TIMESTAMP_KEY = "timestamp";

  public interface CloseableIterator<T> extends Iterator<T>
  {
    void close();
  }

  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final ResponseContext responseContext,
      @Nullable final QueryMetrics<ScanQuery> queryMetrics
  )
  {
    // "legacy" should be non-null due to toolChest.mergeResults
    final boolean legacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");
    SegmentScanProfile profile = new SegmentScanProfile(segment.getId());
    profile.batchSize = query.getBatchSize();
    responseContext.getProfileStack().leaf(profile);
    Timer timer = Timer.createStarted();

    // If the query has a row limit, and the query execution has already
    // returned at least that number of rows, then just return an empty sequence.
    final Long numScannedRows = responseContext.getRowScanCount();
    if (numScannedRows != null && numScannedRows >= query.getScanRowsLimit() && query.getOrder().equals(ScanQuery.Order.NONE)) {
      profile.limited = true;
      profile.timeNs = timer.get();
      return Sequences.empty();
    }
    final boolean hasTimeout = QueryContexts.hasTimeout(query);
    final Long timeoutAt = responseContext.getTimeoutTime();
    final long start = System.currentTimeMillis();
    final StorageAdapter adapter = segment.asStorageAdapter();

    if (adapter == null) {
      String msg = "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped.";
      profile.error = msg;
      throw new ISE(msg);
    }

    final List<String> allColumns = new ArrayList<>();
    if (query.getColumns() != null && !query.getColumns().isEmpty()) {
      // The query provides the set of columns to return.
      if (legacy && !query.getColumns().contains(LEGACY_TIMESTAMP_KEY)) {
        allColumns.add(LEGACY_TIMESTAMP_KEY);
      }

      // Unless we're in legacy mode, allColumns equals query.getColumns() exactly. This is nice since it makes
      // the compactedList form easier to use.
      allColumns.addAll(query.getColumns());
    } else {
      // No columns specified in the query. This is the equivalent of a SELECT *:
      // include all columns from the datasource.
      profile.isWildcard = true;
      final Set<String> availableColumns = Sets.newLinkedHashSet(
          Iterables.concat(
              Collections.singleton(legacy ? LEGACY_TIMESTAMP_KEY : ColumnHolder.TIME_COLUMN_NAME),
              Iterables.transform(
                  Arrays.asList(query.getVirtualColumns().getVirtualColumns()),
                  VirtualColumn::getOutputName
              ),
              adapter.getAvailableDimensions(),
              adapter.getAvailableMetrics()
          )
      );

      allColumns.addAll(availableColumns);

      if (legacy) {
        allColumns.remove(ColumnHolder.TIME_COLUMN_NAME);
      }
    }
    profile.columnCount = allColumns.size();

    final List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    try {
      Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);
    }
    catch (IllegalArgumentException e) {
      profile.error = e.getMessage();
      throw e;
    }

    Interval interval = intervals.get(0);
    profile.interval = interval.toString();
    final SegmentId segmentId = segment.getId();

    final Filter filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));

    // If the row count is not set, set it to 0, else do nothing.
    responseContext.addRowScanCount(0);
    final long limit = calculateRemainingScanRowsLimit(query, responseContext);
    return Sequences.concat(
            adapter
                .makeCursors(
                    filter,
                    interval,
                    query.getVirtualColumns(),
                    Granularities.ALL,
                    query.getOrder().equals(ScanQuery.Order.DESCENDING) ||
                    (query.getOrder().equals(ScanQuery.Order.NONE) && query.isDescending()),
                    QueryMetricsAdapter.wrap(queryMetrics, new InnerProfileStack(profile))
                )
                .map(cursor -> new BaseSequence<>(
                    new BaseSequence.IteratorMaker<ScanResultValue, Iterator<ScanResultValue>>()
                    {
                      @Override
                      public Iterator<ScanResultValue> make()
                      {
                        final List<BaseObjectColumnValueSelector<?>> columnSelectors = new ArrayList<>(allColumns.size());

                        for (String column : allColumns) {
                          final BaseObjectColumnValueSelector<?> selector;

                          if (legacy && LEGACY_TIMESTAMP_KEY.equals(column)) {
                            selector = cursor.getColumnSelectorFactory()
                                             .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
                          } else {
                            selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
                          }

                          columnSelectors.add(selector);
                        }

                        final int batchSize = query.getBatchSize();
                        return new CloseableIterator<ScanResultValue>()
                        {
                          private long offset = 0;

                          @Override
                          public boolean hasNext()
                          {
                            return !cursor.isDone() && offset < limit;
                          }

                          @Override
                          public ScanResultValue next()
                          {
                            timer.start();
                            if (!hasNext()) {
                              throw new NoSuchElementException();
                            }
                            if (hasTimeout && System.currentTimeMillis() >= timeoutAt) {
                              String msg = StringUtils.nonStrictFormat("Query [%s] timed out", query.getId());
                              profile.error = msg;
                              throw new QueryTimeoutException(msg);
                            }
                            final long lastOffset = offset;
                            final Object events;
                            final ScanQuery.ResultFormat resultFormat = query.getResultFormat();
                            if (ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST.equals(resultFormat)) {
                              events = rowsToCompactedList();
                            } else if (ScanQuery.ResultFormat.RESULT_FORMAT_LIST.equals(resultFormat)) {
                              events = rowsToList();
                            } else {
                              UOE e = new UOE("resultFormat[%s] is not supported", resultFormat.toString());
                              profile.error = e.getMessage();
                              throw e;
                            }
                            responseContext.addRowScanCount(offset - lastOffset);
                            profile.rows += offset - lastOffset;
                            if (hasTimeout) {
                              responseContext.putTimeoutTime(
                                  timeoutAt - (System.currentTimeMillis() - start)
                              );
                            }
                            timer.stop();
                            return new ScanResultValue(segmentId.toString(), allColumns, events);
                          }

                          @Override
                          public void remove()
                          {
                            throw new UnsupportedOperationException();
                          }

                          private List<List<Object>> rowsToCompactedList()
                          {
                            final List<List<Object>> events = new ArrayList<>(batchSize);
                            final long iterLimit = Math.min(limit, offset + batchSize);
                            for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
                              final List<Object> theEvent = new ArrayList<>(allColumns.size());
                              for (int j = 0; j < allColumns.size(); j++) {
                                theEvent.add(getColumnValue(j));
                              }
                              events.add(theEvent);
                            }
                            return events;
                          }

                          private List<Map<String, Object>> rowsToList()
                          {
                            List<Map<String, Object>> events = Lists.newArrayListWithCapacity(batchSize);
                            final long iterLimit = Math.min(limit, offset + batchSize);
                            for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
                              final Map<String, Object> theEvent = new LinkedHashMap<>();
                              for (int j = 0; j < allColumns.size(); j++) {
                                theEvent.put(allColumns.get(j), getColumnValue(j));
                              }
                              events.add(theEvent);
                            }
                            return events;
                          }

                          private Object getColumnValue(int i)
                          {
                            final BaseObjectColumnValueSelector<?> selector = columnSelectors.get(i);
                            final Object value;

                            if (legacy && allColumns.get(i).equals(LEGACY_TIMESTAMP_KEY)) {
                              value = DateTimes.utc((Long) selector.getObject());
                            } else {
                              value = selector == null ? null : selector.getObject();
                            }

                            return value;
                          }

                          @Override
                          public void close()
                          {
                            cursor.close();
                          }
                        };
                      }

                      @Override
                      public void cleanup(Iterator<ScanResultValue> iterFromMake)
                      {
                        profile.timeNs = timer.get();
                        ((CloseableIterator<ScanResultValue>) iterFromMake).close();
                      }
                    }
            ))
    );
  }

  /**
   * If we're performing time-ordering, we want to scan through the first `limit` rows in each segment ignoring the number
   * of rows already counted on other segments.
   */
  private long calculateRemainingScanRowsLimit(ScanQuery query, ResponseContext responseContext)
  {
    Long rowScanCount = responseContext.getRowScanCount();
    if (rowScanCount != null && query.getOrder().equals(ScanQuery.Order.NONE)) {
      return query.getScanRowsLimit() - rowScanCount;
    }
    return query.getScanRowsLimit();
  }
}
