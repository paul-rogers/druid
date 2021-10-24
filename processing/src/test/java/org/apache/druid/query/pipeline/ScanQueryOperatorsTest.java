package org.apache.druid.query.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.scan.ScanResultValue;
import org.junit.Test;

import com.google.common.base.Strings;

public class ScanQueryOperatorsTest {

  @Test
  public void testMockReaderNull() {
    MockScanResultReader.Defn defn = new MockScanResultReader.Defn(0, 0);
    Operator op = MockScanResultReader.FACTORY.build(defn, null, null);
    op.start();
    assertFalse(op.hasNext());
    op.close(false);
  }

  @Test
  public void testMockReaderEmpty() {
    MockScanResultReader.Defn defn = new MockScanResultReader.Defn(0, 1);
    assertFalse(Strings.isNullOrEmpty(defn.segmentId));
    Operator op = MockScanResultReader.FACTORY.build(defn, null, null);
    op.start();
    assertTrue(op.hasNext());
    ScanResultValue value = (ScanResultValue) op.next();
    assertTrue(value.getColumns().isEmpty());
    @SuppressWarnings("unchecked")
    List<List<String>> events = (List<List<String>>) value.getEvents();
    assertEquals(1, events.size());
    assertTrue(events.get(0).isEmpty());
    assertFalse(op.hasNext());
    op.close(false);
  }

  @Test
  public void testMockReader() {
    MockScanResultReader.Defn defn = new MockScanResultReader.Defn(3, 10);
    defn.batchSize = 4;
    Operator op = MockScanResultReader.FACTORY.build(defn, null, null);
    op.start();
    int rowCount = 0;
    while (op.hasNext()) {
      ScanResultValue value = (ScanResultValue) op.next();
      assertEquals(3, value.getColumns().size());
      assertEquals("Column1", value.getColumns().get(0));
      assertEquals("Column2", value.getColumns().get(1));
      assertEquals("Column3", value.getColumns().get(2));
      @SuppressWarnings("unchecked")
      List<List<String>> events = (List<List<String>>) value.getEvents();
      assertTrue(events.size() <= 4);
      for (List<String> row : events) {
        for (int i = 0; i < 3; i++) {
          assertNotNull(row.get(i));
        }
        rowCount++;
      }
    }
    assertEquals(10, rowCount);
    op.close(false);
  }

  @Test
  public void testOffset()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    ScanResultOffsetOperator.register(reg);
    final int totalRows = 10;
    MockScanResultReader.Defn leafDefn = new MockScanResultReader.Defn(3, totalRows);
    leafDefn.batchSize = 4;
    for (int offset = 1; offset < 2 * totalRows; offset++) {
      FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
      ScanResultOffsetOperator.Defn offsetDefn = new ScanResultOffsetOperator.Defn();
      offsetDefn.offset = offset;
      offsetDefn.child = leafDefn;
      runner.build(offsetDefn);
      AtomicInteger rowCount = new AtomicInteger();
      final String firstVal = StringUtils.format("Value %d.0", offset);
      runner.fullRun(row -> {
        ScanResultValue value = (ScanResultValue) row;
        @SuppressWarnings("unchecked")
        List<List<String>> events = (List<List<String>>) value.getEvents();
        if (rowCount.getAndAdd(events.size()) == 0) {
          assertEquals(firstVal, events.get(0).get(0));
        }
        return true;
      });
      assertEquals(Math.max(0, totalRows - offset), rowCount.get());
    }
  }

  @Test
  public void testLimit()
  {
    OperatorRegistry reg = new OperatorRegistry();
    MockScanResultReader.register(reg);
    ScanResultLimitOperator.register(reg);
    FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
    final int totalRows = 10;
    MockScanResultReader.Defn leafDefn = new MockScanResultReader.Defn(3, totalRows);
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
        @SuppressWarnings("unchecked")
        List<List<String>> events = (List<List<String>>) value.getEvents();
        rowCount.getAndAdd(events.size());
        return true;
      });
      assertEquals(Math.min(totalRows, limit), rowCount.get());
    }
  }

}
