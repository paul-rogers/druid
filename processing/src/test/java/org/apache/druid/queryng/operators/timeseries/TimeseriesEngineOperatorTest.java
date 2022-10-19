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

package org.apache.druid.queryng.operators.timeseries;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.timeseries.TimeseriesResultBuilder;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.general.CursorDefinition;
import org.apache.druid.queryng.operators.general.MockStorageAdapter;
import org.apache.druid.queryng.operators.general.MockStorageAdapter.MockSegment;
import org.apache.druid.segment.VirtualColumns;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TimeseriesEngineOperatorTest
{
  static {
    NullHandling.initializeForTests();
  }

  private CursorDefinition mockCursor(int rowCount, int cursorCount, boolean sparse)
  {
    return new CursorDefinition(
        new MockSegment(rowCount, cursorCount, sparse),
        MockStorageAdapter.MOCK_INTERVAL, // Whole segment
        null, // No filter
        VirtualColumns.EMPTY,
        false,
        Granularities.HOUR,
        null, // No query metrics
        0 // No vector
    );
  }

  @Test
  public void testBasics()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Result<TimeseriesResultValue>> op = new TimeseriesEngineOperator(
        fragment,
        mockCursor(4, 1, false),
        Arrays.asList(
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("sum", "delta")
        ),
        null, // No buffer pool: not vectorized
        false // don't skip empty buckets
    );
    fragment.registerRoot(op);
    List<Result<TimeseriesResultValue>> results = fragment.toList();
    assertEquals(1, results.size());
    Result<TimeseriesResultValue> value = results.get(0);
    Result<TimeseriesResultValue> expected = new TimeseriesResultBuilder(
          MockStorageAdapter.MOCK_INTERVAL.getStart())
        .addMetric("cnt", 4L)
        .addMetric("sum", 6L)
        .build();
    assertEquals(expected, value);
  }

  @Test
  public void testEmptyResults()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Result<TimeseriesResultValue>> op = new TimeseriesEngineOperator(
        fragment,
        mockCursor(4, 0, false),
        Arrays.asList(
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("sum", "delta")
        ),
        null, // No buffer pool: not vectorized
        false // don't skip empty buckets
    );
    fragment.registerRoot(op);
    List<Result<TimeseriesResultValue>> results = fragment.toList();
    assertEquals(0, results.size());
  }

  @Test
  public void testSingleRowPerGrainResults()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Result<TimeseriesResultValue>> op = new TimeseriesEngineOperator(
        fragment,
        mockCursor(1, 2, false),
        Arrays.asList(
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("sum", "delta")
        ),
        null, // No buffer pool: not vectorized
        false // don't skip empty buckets
    );
    fragment.registerRoot(op);
    List<Result<TimeseriesResultValue>> results = fragment.toList();
    assertEquals(2, results.size());
    Result<TimeseriesResultValue> value = results.get(0);
    assertEquals(MockStorageAdapter.MOCK_INTERVAL.getStart(), value.getTimestamp());
    value = results.get(1);
    assertEquals(MockStorageAdapter.MOCK_INTERVAL.getStart().plusHours(1), value.getTimestamp());
  }

  @Test
  public void testRowAggWithinGrain()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Result<TimeseriesResultValue>> op = new TimeseriesEngineOperator(
        fragment,
        mockCursor(4, 2, false),
        Arrays.asList(
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("sum", "delta")
        ),
        null, // No buffer pool: not vectorized
        false // don't skip empty buckets
    );
    fragment.registerRoot(op);
    List<Result<TimeseriesResultValue>> results = fragment.toList();
    assertEquals(2, results.size());
    Result<TimeseriesResultValue> expected = new TimeseriesResultBuilder(
        MockStorageAdapter.MOCK_INTERVAL.getStart())
      .addMetric("cnt", 2L)
      .addMetric("sum", 1L)
      .build();
    assertEquals(expected, results.get(0));
    expected = new TimeseriesResultBuilder(
        MockStorageAdapter.MOCK_INTERVAL.getStart().plusHours(1))
      .addMetric("cnt", 2L)
      .addMetric("sum", 5L)
      .build();
    assertEquals(expected, results.get(1));
  }

  @Test
  public void testSparseRowsWithZeroFill()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Result<TimeseriesResultValue>> op = new TimeseriesEngineOperator(
        fragment,
        mockCursor(4, 3, true),
        Arrays.asList(
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("sum", "delta")
        ),
        null, // No buffer pool: not vectorized
        false // don't skip empty buckets
    );
    fragment.registerRoot(op);
    List<Result<TimeseriesResultValue>> results = fragment.toList();
    System.out.println(results);
    assertEquals(3, results.size());
    Result<TimeseriesResultValue> expected = new TimeseriesResultBuilder(
        MockStorageAdapter.MOCK_INTERVAL.getStart())
      .addMetric("cnt", 2L)
      .addMetric("sum", 1L)
      .build();
    assertEquals(expected, results.get(0));
    expected = new TimeseriesResultBuilder(
        MockStorageAdapter.MOCK_INTERVAL.getStart().plusHours(1))
      .addMetric("cnt", 0L)
      .addMetric("sum", 0L)
      .build();
    assertEquals(expected, results.get(1));
    expected = new TimeseriesResultBuilder(
        MockStorageAdapter.MOCK_INTERVAL.getStart().plusHours(2))
      .addMetric("cnt", 2L)
      .addMetric("sum", 1L)
      .build();
    assertEquals(expected, results.get(2));
  }

  @Test
  public void testSparseRowsWithoutZeroFill()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Result<TimeseriesResultValue>> op = new TimeseriesEngineOperator(
        fragment,
        mockCursor(4, 3, true),
        Arrays.asList(
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("sum", "delta")
        ),
        null, // No buffer pool: not vectorized
        true // skip empty buckets
    );
    fragment.registerRoot(op);
    List<Result<TimeseriesResultValue>> results = fragment.toList();
    System.out.println(results);
    assertEquals(2, results.size());
    Result<TimeseriesResultValue> expected = new TimeseriesResultBuilder(
        MockStorageAdapter.MOCK_INTERVAL.getStart())
      .addMetric("cnt", 2L)
      .addMetric("sum", 1L)
      .build();
    assertEquals(expected, results.get(0));
    expected = new TimeseriesResultBuilder(
        MockStorageAdapter.MOCK_INTERVAL.getStart().plusHours(2))
      .addMetric("cnt", 2L)
      .addMetric("sum", 1L)
      .build();
    assertEquals(expected, results.get(1));
  }
}
