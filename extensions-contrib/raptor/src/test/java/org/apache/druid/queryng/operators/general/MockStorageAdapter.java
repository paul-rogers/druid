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

package org.apache.druid.queryng.operators.general;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Metadata;
import org.apache.druid.segment.QueryableIndex;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.data.Indexed;
import org.apache.druid.segment.data.ListIndexed;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class MockStorageAdapter implements StorageAdapter
{
  public static final Interval MOCK_INTERVAL = Intervals.of("2015-09-12T13:00:00.000Z/2015-09-12T14:00:00.000Z");
  public static final SegmentDescriptor MOCK_DESCRIPTOR = new SegmentDescriptor(MOCK_INTERVAL, "1", 1);

  public static class MockColumn
  {
    final Comparable<?> minValue;
    final Comparable<?> maxValue;
    final ColumnCapabilities capabilities;

    public MockColumn(
        Comparable<?> minValue,
        Comparable<?> maxValue,
        ColumnCapabilities capabilities
    )
    {
      this.minValue = minValue;
      this.maxValue = maxValue;
      this.capabilities = capabilities;
    }
  }

  public static class MockSegment implements Segment
  {
    protected final int segmentSize;
    protected final int cursorCount;
    protected final boolean sparse;

    public MockSegment(int segmentSize)
    {
      this(segmentSize, 1, false);
    }

    public MockSegment(int segmentSize, int cursorCount, boolean sparse)
    {
      this.segmentSize = segmentSize;
      this.cursorCount = cursorCount;
      this.sparse = sparse;
    }

    @Override
    public void close()
    {
    }

    @Override
    public SegmentId getId()
    {
      return SegmentId.of(
          "dummyDS",
          MOCK_DESCRIPTOR.getInterval(),
          MOCK_DESCRIPTOR.getVersion(),
          MOCK_DESCRIPTOR.getPartitionNumber()
      );
    }

    @Override
    public Interval getDataInterval()
    {
      return MockStorageAdapter.MOCK_INTERVAL;
    }

    @Override
    public QueryableIndex asQueryableIndex()
    {
      return null;
    }

    @Override
    public StorageAdapter asStorageAdapter()
    {
      if (segmentSize < 0) {
        // Simulate no segment available
        return null;
      }
      return new MockStorageAdapter(this);
    }
  }

  private MockSegment segment;
  protected final Map<String, MockColumn> columns;

  public MockStorageAdapter()
  {
    this(new MockSegment(5_000_000));
  }

  public MockStorageAdapter(MockSegment segment)
  {
    this.segment = segment;
    ImmutableMap.Builder<String, MockColumn> builder = ImmutableMap.builder();
    builder.put(
        ColumnHolder.TIME_COLUMN_NAME,
        new MockColumn(
            getMinTime(),
            getMaxTime(),
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG)
        )
    );
    builder.put(
        "delta",
        new MockColumn(
            0,
            10_000,
            ColumnCapabilitiesImpl.createSimpleNumericColumnCapabilities(ColumnType.LONG)
        )
    );
    builder.put(
        "page",
        new MockColumn(
            "",
            "zzzzzzzzzz",
            ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
        )
    );
    builder.put(
        "dim",
        new MockColumn(
            "",
            "zzzzzzzzzz",
            ColumnCapabilitiesImpl.createSimpleSingleValueStringColumnCapabilities()
        )
    );
    this.columns = builder.build();
  }

  @Override
  public Sequence<Cursor> makeCursors(
      final Filter filter,
      final Interval interval,
      final VirtualColumns virtualColumns,
      final Granularity gran,
      final boolean descending,
      final QueryMetrics<?> queryMetrics)
  {
    final int rowsPerCursor;
    if (segment.cursorCount == 0) {
      rowsPerCursor = 0;
    } else if (segment.sparse) {
      rowsPerCursor = segment.segmentSize / (segment.cursorCount + 1) * 2;
    } else {
      rowsPerCursor = segment.segmentSize / segment.cursorCount;
    }
    final Interval actualInterval = computeCursorInterval(gran, interval);
    long nextCursorStart = actualInterval.getStartMillis();
    long increment = gran.increment(0);

    // Not very realistic, but good enough for testing. If the granularity
    // is larger than the segment span, just punt and pretend the granularity
    // is smaller. Needed so we can test multiple cursors with the scan query.
    // In practice, the scan query should get just one cursor, because it gives
    // the all-time granularity, but we want to test the multi-cursor path anyway.
    if (increment > actualInterval.toDurationMillis()) {
      int blockCount = segment.sparse ? 2 * segment.cursorCount - 1 : segment.cursorCount;
      increment = actualInterval.toDurationMillis() / blockCount;
    }
    final List<Cursor> cursors = new ArrayList<>();
    for (int i = 0; i < segment.cursorCount; i++) {
      Interval cursorInterval = new Interval(nextCursorStart, nextCursorStart + increment, DateTimeZone.UTC);
      int rows = (segment.sparse && i % 2 == 1) ? 0 : rowsPerCursor;
      cursors.add(new MockCursor(this, cursorInterval, rows));
      nextCursorStart += increment;
    }
    return Sequences.simple(cursors);
  }

  private Interval computeCursorInterval(final Granularity gran, final Interval interval)
  {
    final DateTime minTime = getMinTime();
    final DateTime maxTime = getMaxTime();
    final Interval dataInterval = new Interval(minTime, gran.bucketEnd(maxTime));

    if (!interval.overlaps(dataInterval)) {
      return null;
    }

    return interval.overlap(dataInterval);
  }

  @Override
  public Interval getInterval()
  {
    return segment.getDataInterval();
  }

  @Override
  public Indexed<String> getAvailableDimensions()
  {
    return new ListIndexed<>(Arrays.asList(ColumnHolder.TIME_COLUMN_NAME, "page"));
  }

  @Override
  public Iterable<String> getAvailableMetrics()
  {
    return Collections.singletonList("delta");
  }

  @Override
  public int getDimensionCardinality(String column)
  {
    return segment.segmentSize;
  }

  @Override
  public DateTime getMinTime()
  {
    return segment.getDataInterval().getStart();
  }

  @Override
  public DateTime getMaxTime()
  {
    return segment.getDataInterval().getEnd();
  }

  @Override
  public Comparable<?> getMinValue(String column)
  {
    MockColumn col = columns.get(column);
    return col == null ? null : col.minValue;
  }

  @Override
  public Comparable<?> getMaxValue(String column)
  {
    MockColumn col = columns.get(column);
    return col == null ? null : col.maxValue;
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    MockColumn col = columns.get(column);
    return col == null ? null : col.capabilities;
  }

  @Override
  public int getNumRows()
  {
    return segment.segmentSize;
  }

  @Override
  public DateTime getMaxIngestedEventTime()
  {
    return getMaxTime();
  }

  @Override
  public Metadata getMetadata()
  {
    return null;
  }
}
