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
    op.start();
    assertFalse(op.hasNext());
    op.close(false);
  }

  @Test
  public void testMockReaderEmpty() {
    MockScanResultReader.Defn defn = scanDefn(0, 1);
    assertFalse(Strings.isNullOrEmpty(defn.segmentId));
    Operator op = MockScanResultReader.FACTORY.build(defn, null, null);
    op.start();
    assertTrue(op.hasNext());
    ScanResultValue value = (ScanResultValue) op.next();
    assertTrue(value.getColumns().isEmpty());
    List<List<String>> events = value.getRows();
    assertEquals(1, events.size());
    assertTrue(events.get(0).isEmpty());
    assertFalse(op.hasNext());
    op.close(false);
  }

  @Test
  public void testMockReader() {
    MockScanResultReader.Defn defn = scanDefn(3, 10);
    defn.batchSize = 4;
    Operator op = MockScanResultReader.FACTORY.build(defn, null, null);
    op.start();
    int rowCount = 0;
    while (op.hasNext()) {
      ScanResultValue value = (ScanResultValue) op.next();
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

    assertEquals(MockScanResultReader.State.NEW, firstLeaf.state);
    assertEquals(MockScanResultReader.State.NEW, secondLeaf.state);
    assertTrue(concat.hasNext());
    assertEquals(MockScanResultReader.State.RUN, firstLeaf.state);
    assertEquals(MockScanResultReader.State.NEW, secondLeaf.state);
    concat.next();
    assertTrue(concat.hasNext());
    assertEquals(MockScanResultReader.State.RUN, firstLeaf.state);
    assertEquals(MockScanResultReader.State.NEW, secondLeaf.state);
    concat.next();
    assertTrue(concat.hasNext());
    assertEquals(MockScanResultReader.State.CLOSED, firstLeaf.state);
    assertEquals(MockScanResultReader.State.RUN, secondLeaf.state);
    concat.next();
    assertTrue(concat.hasNext());
    assertEquals(MockScanResultReader.State.RUN, secondLeaf.state);
    concat.next();
    assertFalse(concat.hasNext());
    assertEquals(MockScanResultReader.State.CLOSED, secondLeaf.state);
  }

  // Value of descriptor not used in test, just keeps the query builder
  // and setup code happy
  private static final SegmentDescriptor DUMMY_DESCRIPTOR = new SegmentDescriptor(
      Intervals.of("2012-01-01T00:00:00Z/P1D"),
      "version",
      0
  );
  /**
   * Test an ascending sort with various numbers of rows.
   * Note that, as currently implemented, the sort converts a batch of
   * compact lists into an inefficient set of batches with single values.
   * So, the start and end times of each "batch" (single row) will be
   * the same.
   */
  @Test
  public void testSortAsc()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    ScanResultSortOperator.register(reg);
    final int rowsPerStep = 10;
    ScanQuery query = new ScanQueryBuilder()
        .dataSource("dummy")
        .intervals(new SpecificSegmentSpec(DUMMY_DESCRIPTOR))
        .order(ScanQuery.Order.ASCENDING)
        .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();
    for (int i = 0; i < 5; i++) {
      FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
      final int expectedRows = rowsPerStep * i;
      MockScanResultReader.Defn leafDefn = scanDefn(3, expectedRows);
      leafDefn.batchSize = 4;
      ScanResultSortOperator.Defn sortDefn = new ScanResultSortOperator.Defn(query, leafDefn);
      runner.build(sortDefn);
      AtomicInteger rowCount = new AtomicInteger();
      AtomicLong lastTs = new AtomicLong();
      runner.fullRun(row -> {
        ScanResultValue value = (ScanResultValue) row;
        rowCount.getAndAdd(value.rowCount());
        long startTs = MockScanResultReader.getFirstTime(row);
        long endTs = MockScanResultReader.getLastTime(row);
        assertTrue(startTs <= endTs);
        assertTrue(lastTs.get() < startTs);
        lastTs.set(endTs);
        return true;
      });
      assertEquals(expectedRows, rowCount.get());
    }
  }

  @Test
  public void testSortDesc()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    ScanResultSortOperator.register(reg);
    final int rowsPerStep = 10;
    ScanQuery query = new ScanQueryBuilder()
        .dataSource("dummy")
        .intervals(new SpecificSegmentSpec(DUMMY_DESCRIPTOR))
        .order(ScanQuery.Order.DESCENDING)
        .resultFormat(ResultFormat.RESULT_FORMAT_COMPACTED_LIST)
        .build();
    for (int i = 0; i < 5; i++) {
      FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
      final int expectedRows = rowsPerStep * i;
      MockScanResultReader.Defn leafDefn = scanDefn(3, expectedRows);
      leafDefn.batchSize = 4;
      ScanResultSortOperator.Defn sortDefn = new ScanResultSortOperator.Defn(query, leafDefn);
      runner.build(sortDefn);
      AtomicInteger rowCount = new AtomicInteger();
      AtomicLong lastTs = new AtomicLong(Long.MAX_VALUE);
      runner.fullRun(row -> {
        ScanResultValue value = (ScanResultValue) row;
        rowCount.getAndAdd(value.rowCount());
        long startTs = MockScanResultReader.getFirstTime(row);
        long endTs = MockScanResultReader.getLastTime(row);
        assertTrue(startTs >= endTs);
        assertTrue(lastTs.get() > startTs);
        lastTs.set(endTs);
        return true;
      });
      assertEquals(expectedRows, rowCount.get());
    }
  }
}
