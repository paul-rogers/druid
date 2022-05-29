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
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.scan.ScanResultLimitOperator;
import org.apache.druid.queryng.operators.scan.ScanResultOffsetOperator;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.List;

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

  private MockScanResultReader scan(int columnCount, int rowCount, int batchSize)
  {
    return new MockScanResultReader(columnCount, rowCount, batchSize, interval(0));
  }

  // Tests for the mock reader used to power tests without the overhead
  // of using an actual scan operator. The parent operators don't know
  // the difference.
  @Test
  public void testMockReaderNull()
  {
    final FragmentContext context = FragmentContext.defaultContext();
    Operator op = scan(0, 0, 3);
    Iterator<Object> iter = op.open(context);
    assertFalse(iter.hasNext());
    op.close(false);
  }

  @Test
  public void testMockReaderEmpty()
  {
    final FragmentContext context = FragmentContext.defaultContext();
    MockScanResultReader scan = scan(0, 1, 3);
    assertFalse(Strings.isNullOrEmpty(scan.segmentId));
    Iterator<Object> iter = scan.open(context);
    assertTrue(iter.hasNext());
    ScanResultValue value = (ScanResultValue) iter.next();
    assertTrue(value.getColumns().isEmpty());
    List<List<String>> events = value.getRows();
    assertEquals(1, events.size());
    assertTrue(events.get(0).isEmpty());
    assertFalse(iter.hasNext());
    scan.close(false);
  }

  @Test
  public void testMockReader()
  {
    final FragmentContext context = FragmentContext.defaultContext();
    Operator scan = scan(3, 10, 4);
    Iterator<Object> iter = scan.open(context);
    int rowCount = 0;
    while (iter.hasNext()) {
      ScanResultValue value = (ScanResultValue) iter.next();
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
    scan.close(false);
  }

  /**
   * Test the offset operator for various numbers of input rows, spread across
   * multiple batches. Tests for offsets that fall on a batch boundary
   * and within a batch.
   */
  @Test
  public void testOffset()
  {
    final FragmentContext context = FragmentContext.defaultContext();
    final int totalRows = 10;
    for (int offset = 1; offset < 2 * totalRows; offset++) {
      Operator scan = scan(3, totalRows, 4);
      Operator root = new ScanResultOffsetOperator(offset, scan);
      int rowCount = 0;
      final String firstVal = StringUtils.format("Value %d.1", offset);
      for (Object row : Operators.toIterable(root, context)) {
        ScanResultValue value = (ScanResultValue) row;
        List<List<Object>> events = value.getRows();
        if (rowCount == 0) {
          assertEquals(firstVal, events.get(0).get(1));
        }
        rowCount += events.size();
      }
      assertEquals(Math.max(0, totalRows - offset), rowCount);
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
    final FragmentContext context = FragmentContext.defaultContext();
    final int totalRows = 10;
    for (int limit = 0; limit < totalRows + 1; limit++) {
      Operator scan = scan(3, totalRows, 4);
      ScanResultLimitOperator root = new ScanResultLimitOperator(limit, true, 4, scan);
      int rowCount = 0;
      for (Object row : Operators.toIterable(root, context)) {
        ScanResultValue value = (ScanResultValue) row;
        rowCount += value.rowCount();
      }
      assertEquals(Math.min(totalRows, limit), rowCount);
    }
  }
}
