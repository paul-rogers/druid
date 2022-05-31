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

package org.apache.druid.queryng.operators;

import com.google.common.base.Strings;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentBuilder;
import org.apache.druid.queryng.fragment.FragmentBuilder.ResultIterator;
import org.apache.druid.queryng.operators.scan.ScanBatchToRowOperator;
import org.apache.druid.queryng.operators.scan.ScanCompactListToArrayOperator;
import org.apache.druid.queryng.operators.scan.ScanListToArrayOperator;
import org.apache.druid.queryng.operators.scan.ScanResultLimitOperator;
import org.apache.druid.queryng.operators.scan.ScanResultOffsetOperator;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ScanQueryOperatorsTest
{
  private Interval interval(int offset)
  {
    Duration grain = Duration.ofMinutes(1);
    Instant base = Instant.parse("2021-10-24T00:00:00Z");
    Duration grainOffset = grain.multipliedBy(offset);
    Instant start = base.plus(grainOffset);
    return new Interval(start.toEpochMilli(), start.plus(grain).toEpochMilli());
  }

  private MockScanResultReader scan(FragmentBuilder builder, int columnCount, int rowCount, int batchSize)
  {
    return new MockScanResultReader(builder, columnCount, rowCount, batchSize, interval(0));
  }

  private MockScanResultReader scan(FragmentBuilder builder, int columnCount, int rowCount, int batchSize, ResultFormat rowFormat)
  {
    return new MockScanResultReader(builder, columnCount, rowCount, batchSize, interval(0), rowFormat);
  }

  // Tests for the mock reader used to power tests without the overhead
  // of using an actual scan operator. The parent operators don't know
  // the difference.
  @Test
  public void testMockReaderNull()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    Operator<ScanResultValue> op = scan(builder, 0, 0, 3);
    ResultIterator<ScanResultValue> iter = builder.open(op);
    assertFalse(iter.hasNext());
    iter.close();
  }

  @Test
  public void testMockReaderEmpty()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockScanResultReader scan = scan(builder, 0, 1, 3);
    assertFalse(Strings.isNullOrEmpty(scan.segmentId));
    ResultIterator<ScanResultValue> iter = builder.open(scan);
    assertTrue(iter.hasNext());
    ScanResultValue value = iter.next();
    assertTrue(value.getColumns().isEmpty());
    List<List<String>> events = value.getRows();
    assertEquals(1, events.size());
    assertTrue(events.get(0).isEmpty());
    assertFalse(iter.hasNext());
    iter.close();
  }

  @Test
  public void testMockReader()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    Operator<ScanResultValue> scan = scan(builder, 3, 10, 4);
    ResultIterator<ScanResultValue> iter = builder.open(scan);
    int rowCount = 0;
    while (iter.hasNext()) {
      ScanResultValue value = iter.next();
      assertEquals(3, value.getColumns().size());
      assertEquals(ColumnHolder.TIME_COLUMN_NAME, value.getColumns().get(0));
      assertEquals("Column1", value.getColumns().get(1));
      assertEquals("Column2", value.getColumns().get(2));
      List<List<Object>> events = value.getRows();
      assertTrue(events.size() <= 4);
      long prevTs = 0;
      for (List<Object> row : events) {
        for (int i = 0; i < 3; i++) {
          Object colValue = row.get(i);
          assertNotNull(row.get(i));
          if (i == 0) {
            assertTrue(colValue instanceof Long);
            long ts = (Long) colValue;
            assertTrue(ts > prevTs);
            prevTs = ts;
          } else {
            assertTrue(colValue instanceof String);
          }
        }
        rowCount++;
      }
    }
    assertEquals(10, rowCount);
    iter.close();
  }

  /**
   * Test the offset operator for various numbers of input rows, spread across
   * multiple batches. Tests for offsets that fall on a batch boundary
   * and within a batch.
   */
  @Test
  public void testOffset()
  {
    final int totalRows = 10;
    for (int offset = 1; offset < 2 * totalRows; offset++) {
      FragmentBuilder builder = FragmentBuilder.defaultBuilder();
      Operator<ScanResultValue> scan = scan(builder, 3, totalRows, 4);
      Operator<ScanResultValue> root = new ScanResultOffsetOperator(builder, offset, scan);
      int rowCount = 0;
      final String firstVal = StringUtils.format("Value %d.1", offset);
      ResultIterator<ScanResultValue> iter = builder.open(root);
      for (ScanResultValue row : Operators.toIterable(iter)) {
        ScanResultValue value = row;
        List<List<Object>> events = value.getRows();
        if (rowCount == 0) {
          assertEquals(firstVal, events.get(0).get(1));
        }
        rowCount += events.size();
      }
      assertEquals(Math.max(0, totalRows - offset), rowCount);
      iter.close();
    }
  }

  /**
   * Test the limit operator for various numbers of input rows, spread across
   * multiple batches. Tests for limits that fall on a batch boundary
   * and within a batch.
   */
  @Test
  public void testLimit()
  {
    final int totalRows = 10;
    for (int limit = 0; limit < totalRows + 1; limit++) {
      FragmentBuilder builder = FragmentBuilder.defaultBuilder();
      Operator<ScanResultValue> scan = scan(builder, 3, totalRows, 4);
      ScanResultLimitOperator root = new ScanResultLimitOperator(builder, limit, true, 4, scan);
      ResultIterator<ScanResultValue> iter = builder.open(root);
      int rowCount = 0;
      for (ScanResultValue row : Operators.toIterable(iter)) {
        ScanResultValue value = row;
        rowCount += value.rowCount();
      }
      assertEquals(Math.min(totalRows, limit), rowCount);
      iter.close();
    }
  }

  @Test
  public void testBatchToRow()
  {
    {
      FragmentBuilder builder = FragmentBuilder.defaultBuilder();
      MockScanResultReader scan = scan(builder, 3, 25, 4, ResultFormat.RESULT_FORMAT_COMPACTED_LIST);
      Operator<List<Object>> op = new ScanBatchToRowOperator<List<Object>>(builder, scan);
      Operator<Object[]> root = new ScanCompactListToArrayOperator(builder, op, scan.columns);
      List<Object[]> results = builder.toList(root);
      assertEquals(25, results.size());
    }
    {
      FragmentBuilder builder = FragmentBuilder.defaultBuilder();
      MockScanResultReader scan = scan(builder, 3, 25, 4, ResultFormat.RESULT_FORMAT_LIST);
      Operator<Map<String, Object>> op = new ScanBatchToRowOperator<Map<String, Object>>(builder, scan);
      Operator<Object[]> root = new ScanListToArrayOperator(builder, op, scan.columns);
      List<Object[]> results = builder.toList(root);
      assertEquals(25, results.size());
    }
  }
}
