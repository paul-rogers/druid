package org.apache.druid.query.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Druids.ScanQueryBuilder;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.pipeline.Operator.FragmentContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.spec.SpecificSegmentSpec;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.Test;

import com.google.common.base.Strings;

public class ScanQueryOperatorsTest
{
  private final FragmentContext context = Operator.defaultContext();

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
  public void testMockReaderNull() {
    Operator op = scan(0, 0, 3);
    Iterator<Object> iter = op.open(context);
    assertFalse(iter.hasNext());
    op.close(false);
  }

  @Test
  public void testMockReaderEmpty() {
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
  public void testMockReader() {
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

  /**
   * Tests the concat operator for various numbers of inputs.
   */
  @Test
  public void testConcat()
  {
    final int rowsPerReader = 10;
    for (int inputCount = 1; inputCount < 5 + 1; inputCount++) {
      List<Operator> children = new ArrayList<>();
      for (int i = 0; i < inputCount; i++) {
        // Creates uneven batches
        children.add(scan(3, rowsPerReader, 4));
      }
      ConcatOperator root = new ConcatOperator(children);
      int rowCount = 0;
      for (Object row : Operators.toIterable(root, context)) {
        ScanResultValue value = (ScanResultValue) row;
        rowCount += value.rowCount();
      }
      assertEquals(rowsPerReader * inputCount, rowCount);
    }
  }

  /**
   * Test that the concat operator opens its inputs as late as possible, and
   * closes then as early as possible.
   */
  @Test
  public void testConcatLifecycle()
  {
    MockScanResultReader firstLeaf = scan(3, 4, 2);
    MockScanResultReader secondLeaf = scan(3, 4, 2);
    List<Operator> children = Arrays.asList(firstLeaf, secondLeaf);
    ConcatOperator root = new ConcatOperator(children);
    Iterator<Object> iter = root.open(context);

    assertEquals(Operator.State.START, firstLeaf.state);
    assertEquals(Operator.State.START, secondLeaf.state);
    assertTrue(iter.hasNext());
    assertEquals(Operator.State.RUN, firstLeaf.state);
    assertEquals(Operator.State.START, secondLeaf.state);
    iter.next();
    assertTrue(iter.hasNext());
    assertEquals(Operator.State.RUN, firstLeaf.state);
    assertEquals(Operator.State.START, secondLeaf.state);
    iter.next();
    assertTrue(iter.hasNext());
    assertEquals(Operator.State.CLOSED, firstLeaf.state);
    assertEquals(Operator.State.RUN, secondLeaf.state);
    iter.next();
    assertTrue(iter.hasNext());
    assertEquals(Operator.State.RUN, secondLeaf.state);
    iter.next();
    assertFalse(iter.hasNext());
    assertEquals(Operator.State.CLOSED, secondLeaf.state);
    root.close(true);
  }

  // Value of descriptor not used in test, just keeps the query builder
  // and setup code happy
  private static final SegmentDescriptor DUMMY_DESCRIPTOR = new SegmentDescriptor(
      Intervals.of("2012-01-01T00:00:00Z/P1D"),
      "version",
      0
  );

  final int ROWS_PER_STEP = 10;

  private Operator setupSort(int segCount, ScanQuery.Order order)
  {
    ScanQuery query = new ScanQueryBuilder()
        .dataSource("dummy")
        .intervals(new SpecificSegmentSpec(DUMMY_DESCRIPTOR))
        .order(order)
        .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();

    // Create segments in unsorted order
    final int segs[] = new int[]{1, 0, 4, 2, 3};
    final int expectedRows = ROWS_PER_STEP * segCount;
    final Operator input;
    switch (segCount) {
    case 0:
      input = new NullOperator();
      break;
    case 1: {
      input = scan(3, expectedRows, 4);
      break;
    }
    default: {
      List<Operator> leaves = new ArrayList<>();
      for (int j = 0; j < segCount; j++) {
        MockScanResultReader scan = new MockScanResultReader(3, ROWS_PER_STEP, 4, interval(segs[j]));
         leaves.add(scan);
      }
      input = new ConcatOperator(leaves);
    }
    }
    return ScanResultSortOperator.forQuery(query, input);
  }

  /**
   * Test an ascending sort with various numbers of rows.
   */
  @Test
  public void testSortAsc()
  {
    for (int i = 0; i < 5; i++) {
      Operator root = setupSort(i, ScanQuery.Order.ASCENDING);
      int rowCount = 0;
      long lastTs = 0;
      for (Object row : Operators.toIterable(root, context)) {
        ScanResultValue value = (ScanResultValue) row;
        rowCount += value.rowCount();
        long startTs = MockScanResultReader.getFirstTime(row);
        assertTrue(lastTs < startTs);
        lastTs = startTs;
      }
      assertEquals(ROWS_PER_STEP * i, rowCount);
    }
  }

  @Test
  public void testSortDesc()
  {
    for (int i = 0; i < 5; i++) {
      Operator root = setupSort(i, ScanQuery.Order.DESCENDING);
      int rowCount = 0;
      long lastTs = Long.MAX_VALUE;
      for (Object row : Operators.toIterable(root, context)) {
        ScanResultValue value = (ScanResultValue) row;
        rowCount += value.rowCount();
        long startTs = MockScanResultReader.getFirstTime(row);
        assertTrue(lastTs > startTs);
        lastTs = startTs;
      }
      assertEquals(ROWS_PER_STEP * i, rowCount);
    }
  }

  // TODO: Scan with limit. Requires greater attention to intervals.

  @Test
  public void testDeaggregate()
  {
    final int totalRows = 10;
    Operator scan = scan(3, totalRows, 4);
    Operator root = new DisaggregateScanResultOperator(scan);
    int rowCount = 0;
    for (Object row : Operators.toIterable(root, context)) {
      ScanResultValue value = (ScanResultValue) row;
      assertEquals(1, value.rowCount());
      rowCount += value.rowCount();
    }
    assertEquals(totalRows, rowCount);
  }

  @Test
  public void testMerge()
  {
    ScanQuery query = new ScanQueryBuilder()
        .dataSource("dummy")
        .intervals(new SpecificSegmentSpec(DUMMY_DESCRIPTOR))
        .order(ScanQuery.Order.ASCENDING)
        .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();
    final int rowsPerReader = 10;
    final int readerCount = 5;
    List<Operator> inputs = new ArrayList<>();
    for (int i = 0; i < readerCount; i++) {
      Operator scan = scan(3, rowsPerReader, 4);
      Operator disagg = new DisaggregateScanResultOperator(scan);
      inputs.add(disagg);
    }
    Operator root = ScanResultMergeOperator.forQuery(query, inputs);
    int rowCount = 0;
    long lastTs = 0;
    for (Object row : Operators.toIterable(root, context)) {
      ScanResultValue value = (ScanResultValue) row;
      assertEquals(1, value.rowCount());
      rowCount++;
      long startTs = MockScanResultReader.getFirstTime(row);
      assertTrue(lastTs <= startTs);
      lastTs = startTs;
    }
    assertEquals(rowsPerReader * readerCount, rowCount);
  }
}
