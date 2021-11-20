package org.apache.druid.query.scan;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.scan.TransformSequence.InputCreator;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;
import org.junit.Test;

public class ScanQueryLimitSequenceTest
{
  private InputCreator<ScanResultValue> mockInput(int columnCount, int targetCount, int batchSize, Interval interval)
  {
    return new InputCreator<ScanResultValue>() {
      @Override
      public Sequence<ScanResultValue> open()
      {
        return new BaseSequence<>(
            new BaseSequence.IteratorMaker<ScanResultValue, MockScanResults>()
            {
              @Override
              public MockScanResults make()
              {
                return new MockScanResults(columnCount, targetCount, batchSize, interval);
              }

              @Override
              public void cleanup(MockScanResults iterFromMake)
              {
              }
            }
          );
      }
    };
  }

  private Interval interval(int offset)
  {
    Duration grain = Duration.ofMinutes(1);
    Instant base = Instant.parse("2021-10-24T00:00:00Z");
    Duration grainOffset = grain.multipliedBy(offset);
    Instant start = base.plus(grainOffset);
    return new Interval(start.toEpochMilli(), start.plus(grain).toEpochMilli());
  }

  private InputCreator<ScanResultValue> scan(int columnCount, int rowCount, int batchSize)
  {
    return mockInput(columnCount, rowCount, batchSize, interval(0));
  }

  // Tests for the mock reader used to power tests without the overhead
  // of using an actual scan operator. The parent operators don't know
  // the difference.
  @Test
  public void testMockReaderNull()
  {
    InputCreator<ScanResultValue> scan = scan(0, 0, 3);
    List<ScanResultValue> values = scan.open().toList();
    assert(values.isEmpty());
  }

  @Test
  public void testMockReaderEmpty() {
    InputCreator<ScanResultValue> scan = scan(0, 1, 3);
    List<ScanResultValue> values = scan.open().toList();
    assertEquals(1, values.size());
    assertTrue(values.get(0).getColumns().isEmpty());
    assertEquals(1, MockScanResults.getRow(values.get(0)).size());
  }

  @Test
  public void testMockReader() {
    InputCreator<ScanResultValue> scan = scan(3, 10, 4);
    List<ScanResultValue> values = scan.open().toList();
    assertEquals(3, values.size());
    for (ScanResultValue value : values) {
      assertEquals(3, value.getColumns().size());
      assertEquals(ColumnHolder.TIME_COLUMN_NAME, value.getColumns().get(0));
      assertEquals("Column1", value.getColumns().get(1));
      assertEquals("Column2", value.getColumns().get(2));
      List<List<Object>> events = MockScanResults.getRow(value);
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
      }
    }
  }
  /**
   * Test the limit sequence for various numbers of input rows, spread across
   * multiple batches. Tests for limits that fall on a batch boundary
   * and within a batch.
   */
  @Test
  public void testLimit()
  {
    final int totalRows = 10;
    for (int limit = 0; limit < totalRows + 1; limit++) {
      InputCreator<ScanResultValue> scan = scan(3, totalRows, 4);
      ScanQueryLimitSequence root = new ScanQueryLimitSequence(limit, true, 4, scan);
      List<ScanResultValue> values = root.toList();
      int rowCount = 0;
      for (ScanResultValue value : values) {
         rowCount += MockScanResults.getRow(value).size();
      }
      assertEquals(Math.min(totalRows, limit), rowCount);
    }
  }
}
