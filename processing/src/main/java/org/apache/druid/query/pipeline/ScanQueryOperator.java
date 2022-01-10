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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Implements a scan query against a fragment using a storage adapter which may
 * return one or more cursors for the segment. Each cursor is processed using
 * a {@link CursorReader}. The set of cursors is known only at run time.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryEngine}
 */
public class ScanQueryOperator implements Operator
{
  static final String LEGACY_TIMESTAMP_KEY = "timestamp";

  /**
   * Manages the run state for this operator.
   */
  private class Impl implements Iterator<Object>
  {
    private final FragmentContext context;
    private final SequenceIterator<Cursor> iter;
    private final List<String> selectedColumns;
    private final long limit;
    private CursorReader cursorReader;
    private long rowCount;
    @SuppressWarnings("unused")
    private int batchCount;

    private Impl(FragmentContext context)
    {
      this.context = context;
      ResponseContext responseContext = context.responseContext();
      responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, 0L);
      long baseLimit = query.getScanRowsLimit();
      if (limitType() == Limit.GLOBAL) {
        limit = baseLimit - (Long) responseContext.get(ResponseContext.Key.NUM_SCANNED_ROWS);
      } else {
        limit = baseLimit;
      }
      final StorageAdapter adapter = segment.asStorageAdapter();
      if (adapter == null) {
        throw new ISE(
            "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
        );
      }
      if (isWildcard()) {
        selectedColumns = inferColumns(adapter, isLegacy);
      } else {
        selectedColumns = columns;
      }
      iter = SequenceIterator.of(adapter.makeCursors(
              filter,
              interval(),
              query.getVirtualColumns(),
              Granularities.ALL,
              isDescendingOrder(),
              null
          ));
    }

    protected List<String> inferColumns(StorageAdapter adapter, boolean isLegacy)
    {
      List<String> cols = new ArrayList<>();
      final Set<String> availableColumns = Sets.newLinkedHashSet(
          Iterables.concat(
              Collections.singleton(isLegacy ? LEGACY_TIMESTAMP_KEY : ColumnHolder.TIME_COLUMN_NAME),
              Iterables.transform(
                  Arrays.asList(query.getVirtualColumns().getVirtualColumns()),
                  VirtualColumn::getOutputName
              ),
              adapter.getAvailableDimensions(),
              adapter.getAvailableMetrics()
          )
      );

      cols.addAll(availableColumns);

      if (isLegacy) {
        cols.remove(ColumnHolder.TIME_COLUMN_NAME);
      }
      return cols;
    }

    /**
     * Check if another batch of events is available. They are available if
     * we have (or can get) a cursor which has rows, and we are not at the
     * limit set for this operator.
     */
    @Override
    public boolean hasNext()
    {
      while (true) {
        if (cursorReader != null) {
          if (cursorReader.hasNext()) {
            return true; // Happy path
          }
          // Cursor is done or was empty.
          closeCursorReader();
        }
        if (iter == null) {
          // Done previously
          return false;
        }
        if (rowCount > limit) {
          // Reached row limit
          finish();
          return false;
        }
        if (!iter.hasNext()) {
          // No more cursors
          finish();
          return false;
        }
        // Read from the next cursor.
        cursorReader = new CursorReader(
            iter.next(),
            selectedColumns,
            limit - rowCount,
            batchSize,
            query.getResultFormat(),
            isLegacy);
      }
    }

    private void closeCursorReader() {
      if (cursorReader != null) {
        cursorReader = null;
      }
    }

    private void finish()
    {
      closeCursorReader();
      ResponseContext responseContext = context.responseContext();
      responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, rowCount);
    }

    /**
     * Return the next batch of events from a cursor. Enforce the
     * timeout limit.
     */
    @Override
    public Object next()
    {
      context.checkTimeout();
      List<?> result = (List<?>) cursorReader.next();
      batchCount++;
      rowCount += result.size();
      return new ScanResultValue(
          segmentId,
          selectedColumns,
          result
          );
    }
  }

  public static enum Limit
  {
    NONE,
    /**
     * If we're performing time-ordering, we want to scan through the first `limit` rows in each
     * segment ignoring the number of rows already counted on other segments.
     */
    LOCAL,
    GLOBAL
  }

  private final ScanQuery query;
  private final Segment segment;
  private final String segmentId;
  private final List<String> columns;
  private final Filter filter;
  private final boolean isLegacy;
  private final int batchSize;
  private Impl impl;

  public ScanQueryOperator(final ScanQuery query, final Segment segment)
  {
    this.query = query;
    this.segment = segment;
    this.segmentId = segment.getId().toString();
    this.columns = defineColumns(query);
    List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
    Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);
    this.filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));
    this.isLegacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");
    this.batchSize = query.getBatchSize();
  }

  /**
   * Define the query columns when the list is given by the query.
   */
  private List<String> defineColumns(ScanQuery query) {
    List<String> queryCols = query.getColumns();

    // Missing or empty list means wildcard
    if (queryCols == null || queryCols.isEmpty()) {
      return null;
    }
    final List<String> planCols = new ArrayList<>();
    if (query.isLegacy() && !queryCols.contains(LEGACY_TIMESTAMP_KEY)) {
      planCols.add(LEGACY_TIMESTAMP_KEY);
    }

    // Unless we're in legacy mode, planCols equals query.getColumns() exactly. This is nice since it makes
    // the compactedList form easier to use.
    planCols.addAll(queryCols);
    return planCols;
  }

  public boolean isWildcard(ScanQuery query) {
    return (query.getColumns() == null || query.getColumns().isEmpty());
  }

  public boolean isDescendingOrder()
  {
    return query.getOrder().equals(ScanQuery.Order.DESCENDING) ||
        (query.getOrder().equals(ScanQuery.Order.NONE) && query.isDescending());
  }

  public boolean hasTimeout()
  {
    return QueryContexts.hasTimeout(query);
  }

  public boolean isWildcard()
  {
    return columns == null;
  }

  public Limit limitType()
  {
    if (!query.isLimited()) {
      return Limit.NONE;
    } else if (query.getOrder().equals(ScanQuery.Order.NONE)) {
      return Limit.LOCAL;
    } else {
      return Limit.GLOBAL;
    }
  }

  public Interval interval()
  {
    return query.getQuerySegmentSpec().getIntervals().get(0);
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    impl = new Impl(context);
    return impl;
  }

  @Override
  public void close(boolean cascade)
  {
    if (impl != null) {
      impl.closeCursorReader();
    }
    impl = null;
  }
}
