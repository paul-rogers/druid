package org.apache.druid.query.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.Druids.ScanQueryBuilder;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;
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
  private Interval interval(int offset)
  {
    Duration grain = Duration.ofMinutes(1);
    Instant base = Instant.parse("2021-10-24T00:00:00Z");
    Duration grainOffset = grain.multipliedBy(offset);
    Instant start = base.plus(grainOffset);
    return new Interval(start.toEpochMilli(), start.plus(grain).toEpochMilli());
  }

  private MockScanResultReader.Defn scanDefn(int columnCount, int rowCount)
  {
    return new MockScanResultReader.Defn(columnCount, rowCount, interval(0));
  }

  // Tests for the mock reader used to power tests without the overhead
  // of using an actual scan operator. The parent operators don't know
  // the difference.
  @Test
  public void testMockReaderNull() {
    MockScanResultReader.Defn defn = scanDefn(0, 0);
    Operator op = MockScanResultReader.FACTORY.build(defn, null, null);
    Iterator<Object> iter = op.open();
    assertFalse(iter.hasNext());
    op.close(false);
  }

  @Test
  public void testMockReaderEmpty() {
    MockScanResultReader.Defn defn = scanDefn(0, 1);
    assertFalse(Strings.isNullOrEmpty(defn.segmentId));
    Operator op = MockScanResultReader.FACTORY.build(defn, null, null);
    Iterator<Object> iter = op.open();
    assertTrue(iter.hasNext());
    ScanResultValue value = (ScanResultValue) iter.next();
    assertTrue(value.getColumns().isEmpty());
    List<List<String>> events = value.getRows();
    assertEquals(1, events.size());
    assertTrue(events.get(0).isEmpty());
    assertFalse(iter.hasNext());
    op.close(false);
  }

  @Test
  public void testMockReader() {
    MockScanResultReader.Defn defn = scanDefn(3, 10);
    defn.batchSize = 4;
    Operator op = MockScanResultReader.FACTORY.build(defn, null, null);
    Iterator<Object> iter = op.open();
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
    op.close(false);
  }

  /**
   * Test the offset operator for various numbers of input rows, spread across
   * multiple batches. Tests for offsets that fall on a batch boundary
   * and within a batch.
   */
  @Test
  public void testOffset()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    ScanResultOffsetOperator.register(reg);
    final int totalRows = 10;
    MockScanResultReader.Defn leafDefn = scanDefn(3, totalRows);
    leafDefn.batchSize = 4;
    for (int offset = 1; offset < 2 * totalRows; offset++) {
      FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
      ScanResultOffsetOperator.Defn offsetDefn = new ScanResultOffsetOperator.Defn();
      offsetDefn.offset = offset;
      offsetDefn.child = leafDefn;
      runner.build(offsetDefn);
      AtomicInteger rowCount = new AtomicInteger();
      final String firstVal = StringUtils.format("Value %d.1", offset);
      runner.fullRun(row -> {
        ScanResultValue value = (ScanResultValue) row;
        List<List<Object>> events = value.getRows();
        if (rowCount.getAndAdd(events.size()) == 0) {
          assertEquals(firstVal, events.get(0).get(1));
        }
        return true;
      });
      assertEquals(Math.max(0, totalRows - offset), rowCount.get());
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
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    ScanResultLimitOperator.register(reg);
    FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
    final int totalRows = 10;
    MockScanResultReader.Defn leafDefn = scanDefn(3, totalRows);
    leafDefn.batchSize = 4;
    for (int limit = 0; limit < totalRows + 1; limit++) {
      ScanResultLimitOperator.Defn limitDefn = new ScanResultLimitOperator.Defn();
      limitDefn.limit = limit;
      limitDefn.grouped = true;
      limitDefn.child = leafDefn;
      runner.build(limitDefn);
      AtomicInteger rowCount = new AtomicInteger();
      runner.fullRun(row -> {
        ScanResultValue value = (ScanResultValue) row;
        rowCount.getAndAdd(value.rowCount());
        return true;
      });
      assertEquals(Math.min(totalRows, limit), rowCount.get());
    }
  }

  /**
   * Tests the concat operator for various numbers of inputs.
   */
  @Test
  public void testConcat()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    ConcatOperator.register(reg);
    FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
    final int rowsPerReader = 10;
    for (int inputCount = 1; inputCount < 5 + 1; inputCount++) {
      MockScanResultReader.Defn leafDefn = scanDefn(3, rowsPerReader);
      leafDefn.batchSize = 4; // Creates uneven batches
      List<OperatorDefn> children = new ArrayList<>();
      for (int i = 0; i < inputCount; i++) {
        children.add(leafDefn);
      }
      ConcatOperator.Defn root = new ConcatOperator.Defn(children);
      runner.build(root);
      AtomicInteger rowCount = new AtomicInteger();
      runner.fullRun(row -> {
        ScanResultValue value = (ScanResultValue) row;
        rowCount.getAndAdd(value.rowCount());
        return true;
      });
      assertEquals(rowsPerReader * inputCount, rowCount.get());
    }
  }

  /**
   * Test that the concat operator opens its inputs as late as possible, and
   * closes then as early as possible.
   */
  @Test
  public void testConcatLifecycle()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    ConcatOperator.register(reg);
    FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
    MockScanResultReader.Defn leafDefn = scanDefn(3, 4);
    leafDefn.batchSize = 2;
    List<OperatorDefn> children = Arrays.asList(leafDefn, leafDefn);
    ConcatOperator.Defn root = new ConcatOperator.Defn(children);
    ConcatOperator concat = (ConcatOperator) runner.build(root);
    MockScanResultReader firstLeaf = (MockScanResultReader) runner.operator(0);
    MockScanResultReader secondLeaf = (MockScanResultReader) runner.operator(1);

    assertEquals(Operator.State.START, firstLeaf.state);
    assertEquals(Operator.State.START, secondLeaf.state);
    assertTrue(concat.hasNext());
    assertEquals(Operator.State.RUN, firstLeaf.state);
    assertEquals(Operator.State.START, secondLeaf.state);
    concat.next();
    assertTrue(concat.hasNext());
    assertEquals(Operator.State.RUN, firstLeaf.state);
    assertEquals(Operator.State.START, secondLeaf.state);
    concat.next();
    assertTrue(concat.hasNext());
    assertEquals(Operator.State.CLOSED, firstLeaf.state);
    assertEquals(Operator.State.RUN, secondLeaf.state);
    concat.next();
    assertTrue(concat.hasNext());
    assertEquals(Operator.State.RUN, secondLeaf.state);
    concat.next();
    assertFalse(concat.hasNext());
    assertEquals(Operator.State.CLOSED, secondLeaf.state);
  }

  // Value of descriptor not used in test, just keeps the query builder
  // and setup code happy
  private static final SegmentDescriptor DUMMY_DESCRIPTOR = new SegmentDescriptor(
      Intervals.of("2012-01-01T00:00:00Z/P1D"),
      "version",
      0
  );

  final int ROWS_PER_STEP = 10;

  private FragmentRunner setupSort(int segCount, ScanQuery.Order order)
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    NullOperator.register(reg);
    ConcatOperator.register(reg);
    ScanResultSortOperator.register(reg);
    ScanQuery query = new ScanQueryBuilder()
        .dataSource("dummy")
        .intervals(new SpecificSegmentSpec(DUMMY_DESCRIPTOR))
        .order(order)
        .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();

    // Create segments in unsorted order
    final int segs[] = new int[]{1, 0, 4, 2, 3};
    final int expectedRows = ROWS_PER_STEP * segCount;
    final OperatorDefn input;
    switch (segCount) {
    case 0:
      input = NullOperator.DEFN;
      break;
    case 1: {
      MockScanResultReader.Defn leafDefn = scanDefn(3, expectedRows);
      leafDefn.batchSize = 4;
      input = leafDefn;
      break;
    }
    default: {
      List<OperatorDefn> leaves = new ArrayList<>();
      for (int j = 0; j < segCount; j++) {
        MockScanResultReader.Defn leafDefn = new MockScanResultReader.Defn(3, ROWS_PER_STEP, interval(segs[j]));
        leafDefn.batchSize = 4;
        leaves.add(leafDefn);
      }
      input = new ConcatOperator.Defn(leaves);
    }
    }
    OperatorDefn sortDefn = new ScanResultSortOperator.Defn(query, input);
    FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
    runner.build(sortDefn);
    return runner;
  }

  /**
   * Test an ascending sort with various numbers of rows.
   */
  @Test
  public void testSortAsc()
  {
    for (int i = 0; i < 5; i++) {
      FragmentRunner runner = setupSort(i, ScanQuery.Order.ASCENDING);
      AtomicInteger rowCount = new AtomicInteger();
      AtomicLong lastTs = new AtomicLong();
      runner.fullRun(row -> {
        ScanResultValue value = (ScanResultValue) row;
        rowCount.getAndAdd(value.rowCount());
        long startTs = MockScanResultReader.getFirstTime(row);
        assertTrue(lastTs.get() < startTs);
        lastTs.set(startTs);
        return true;
      });
      assertEquals(ROWS_PER_STEP * i, rowCount.get());
    }
  }

  @Test
  public void testSortDesc()
  {
    for (int i = 0; i < 5; i++) {
      FragmentRunner runner = setupSort(i, ScanQuery.Order.DESCENDING);
      AtomicInteger rowCount = new AtomicInteger();
      AtomicLong lastTs = new AtomicLong(Long.MAX_VALUE);
      runner.fullRun(row -> {
        ScanResultValue value = (ScanResultValue) row;
        rowCount.getAndAdd(value.rowCount());
        long startTs = MockScanResultReader.getFirstTime(row);
        assertTrue(lastTs.get() > startTs);
        lastTs.set(startTs);
        return true;
      });
      assertEquals(ROWS_PER_STEP * i, rowCount.get());
    }
  }

  // TODO: Scan with limit. Requires greater attention to intervals.

  @Test
  public void testDeaggregate()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    DisaggregateScanResultOperator.register(reg);
    final int totalRows = 10;
    MockScanResultReader.Defn leafDefn = scanDefn(3, totalRows);
    leafDefn.batchSize = 4;
    FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
    OperatorDefn root = new DisaggregateScanResultOperator.Defn(leafDefn);
    runner.build(root);
    AtomicInteger rowCount = new AtomicInteger();
    runner.fullRun(row -> {
      ScanResultValue value = (ScanResultValue) row;
      assertEquals(1, value.rowCount());
      rowCount.getAndAdd(1);
      return true;
    });
    assertEquals(totalRows, rowCount.get());
  }

  @Test
  public void testMerge()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    DisaggregateScanResultOperator.register(reg);
    ScanResultMergeOperator.register(reg);
    ScanQuery query = new ScanQueryBuilder()
        .dataSource("dummy")
        .intervals(new SpecificSegmentSpec(DUMMY_DESCRIPTOR))
        .order(ScanQuery.Order.ASCENDING)
        .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();
    final int rowsPerReader = 10;
    final int readerCount = 5;
    List<OperatorDefn> inputs = new ArrayList<>();
    for (int i = 0; i < readerCount; i++) {
      MockScanResultReader.Defn leafDefn = scanDefn(3, rowsPerReader);
      leafDefn.batchSize = 4;
      OperatorDefn disagg = new DisaggregateScanResultOperator.Defn(leafDefn);
      inputs.add(disagg);
    }
    OperatorDefn root = new ScanResultMergeOperator.Defn(query, inputs);
    FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
    runner.build(root);
    AtomicInteger rowCount = new AtomicInteger();
    AtomicLong lastTs = new AtomicLong();
    runner.fullRun(row -> {
      ScanResultValue value = (ScanResultValue) row;
      assertEquals(1, value.rowCount());
      rowCount.getAndAdd(1);
      long startTs = MockScanResultReader.getFirstTime(row);
      assertTrue(lastTs.get() <= startTs);
      lastTs.set(startTs);
      return true;
    });
    assertEquals(rowsPerReader * readerCount, rowCount.get());
  }
}
