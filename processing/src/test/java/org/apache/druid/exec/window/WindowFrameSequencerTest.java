package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.test.SimpleDataGenOperator;
import org.apache.druid.exec.test.SimpleDataGenSpec;
import org.apache.druid.exec.test.TestUtils;
import org.apache.druid.exec.window.old.NonPartitioner;
import org.apache.druid.exec.window.old.WindowFrameCursor;
import org.apache.druid.exec.window.old.WindowFrameSequencer;
import org.apache.druid.exec.window.old.WindowFrameCursor.LagCursor;
import org.apache.druid.exec.window.old.WindowFrameCursor.LeadCursor;
import org.apache.druid.exec.window.old.WindowFrameCursor.PrimaryCursor;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class WindowFrameSequencerTest
{
  private SimpleDataGenSpec dataGenSpec(int batchSize, int rowCount)
  {
    return new SimpleDataGenSpec(
        1,
        Arrays.asList("rid"),
        BatchFormat.OBJECT_ARRAY,
        batchSize,
        rowCount
    );
  }

  private BatchOperator dataGen(int batchSize, int rowCount)
  {
    return new SimpleDataGenOperator(
        TestUtils.emptyFragment(),
        dataGenSpec(batchSize, rowCount)
    );
  }

  private void startSequencer(WindowFrameSequencer sequencer)
  {
    for (WindowFrameCursor cursor : sequencer.cursors()) {
      cursor.bindPartitionBounds(NonPartitioner.GLOBAL_BOUNDS);
    }
    sequencer.startPartition();
  }

  /**
   * Test a buffer cursor that iterates over one existing batch.
   * Because the batch exists, the buffer cursor can do the iteration
   * itself.
   */
  @Test
  public void testPrimaryOneBatchPreloaded()
  {
    final int rowCount = 4;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    assertTrue(buffer.loadBatch(0));
    WindowFrameCursor primary = new PrimaryCursor(buffer);
    primary.bindPartitionBounds(NonPartitioner.GLOBAL_BOUNDS);
    primary.startPartition();
    assertFalse(primary.isEOF());
    assertFalse(primary.isValid());
    for (int i = 0; i < rowCount; i++) {
      assertTrue(primary.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
    }
    assertFalse(primary.next());
    assertTrue(primary.isEOF());
  }

  /**
   * Test a buffer cursor that iterates over two batches, loading them
   * on demand. Because we need a listener to help, we use the partition
   * cursor to provide the listener and drive the iteration.
   */
  @Test
  public void testPrimaryTwoBatchsLoadOnDemand()
  {
    final int rowCount = 9;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    WindowFrameCursor primary = new PrimaryCursor(buffer);
    WindowFrameSequencer seq = new WindowFrameSequencer(buffer, primary, Collections.emptyList());
    startSequencer(seq);
    assertFalse(primary.isEOF());
    assertFalse(primary.isValid());
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
    }
    assertFalse(seq.next());
    assertTrue(primary.isEOF());

    // All batches should be unloaded
    assertEquals(0, buffer.size());
  }

  @Test
  public void testLag2TwoBatchsLoadOnDemand()
  {
    final int rowCount = 9;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    WindowFrameCursor primary = new PrimaryCursor(buffer);
    WindowFrameCursor lag = new LagCursor(buffer, 2);
    WindowFrameSequencer seq = new WindowFrameSequencer(
        buffer,
        primary,
        Collections.singletonList(lag)
    );
    startSequencer(seq);
    assertFalse(primary.isEOF());
    assertFalse(primary.isValid());
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < 2) {
        assertTrue(lag.columns().scalar(0).isNull());
      } else {
        assertEquals(i - 1L, lag.columns().scalar(0).getLong());
      }
    }
    assertFalse(seq.next());
    assertTrue(primary.isEOF());
    assertFalse(lag.isEOF());

    // Lag still holding on to the last batch.
    assertEquals(1, buffer.size());
  }

  @Test
  public void testLead2TwoBatchsLoadOnDemand()
  {
    final int rowCount = 9;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    WindowFrameCursor primary = new PrimaryCursor(buffer);
    WindowFrameCursor lead = new LeadCursor(buffer, 2);
    WindowFrameSequencer seq = new WindowFrameSequencer(
        buffer,
        primary,
        Collections.singletonList(lead)
    );
    startSequencer(seq);
    assertFalse(primary.isEOF());
    assertFalse(primary.isValid());
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < rowCount - 2) {
        assertEquals(i + 3L, lead.columns().scalar(0).getLong());
      } else {
        assertTrue(lead.columns().scalar(0).isNull());
      }
    }
    assertFalse(seq.next());
    assertTrue(primary.isEOF());
    assertTrue(lead.isEOF());
    assertEquals(0, buffer.size());
  }

  @Test
  public void testLargeLeadTwoBatchsLoadOnDemand()
  {
    final int rowCount = 9;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    WindowFrameCursor primary = new PrimaryCursor(buffer);
    WindowFrameCursor lead = new LeadCursor(buffer, 20);
    WindowFrameSequencer seq = new WindowFrameSequencer(
        buffer,
        primary,
        Collections.singletonList(lead)
    );
    startSequencer(seq);
    assertFalse(primary.isEOF());
    assertFalse(primary.isValid());
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      assertTrue(lead.columns().scalar(0).isNull());
    }
    assertFalse(seq.next());
    assertTrue(primary.isEOF());
    assertTrue(lead.isEOF());
    assertEquals(0, buffer.size());
  }

  @Test
  public void testLag2Lead2TwoBatchsLoadOnDemand()
  {
    final int rowCount = 9;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    WindowFrameCursor primary = new PrimaryCursor(buffer);
    WindowFrameCursor lag = new LagCursor(buffer, 2);
    WindowFrameCursor lead = new LeadCursor(buffer, 2);
    WindowFrameSequencer seq = new WindowFrameSequencer(
        buffer,
        primary,
        Arrays.asList(lag, lead)
    );
    startSequencer(seq);
    assertFalse(primary.isEOF());
    assertFalse(primary.isValid());
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < 2) {
        assertTrue(lag.columns().scalar(0).isNull());
      } else {
        assertEquals(i - 1L, lag.columns().scalar(0).getLong());
      }
      if (i < rowCount - 2) {
        assertEquals(i + 3L, lead.columns().scalar(0).getLong());
      } else {
        assertTrue(lead.columns().scalar(0).isNull());
      }
    }
    assertFalse(seq.next());
    assertTrue(primary.isEOF());
    assertTrue(lead.isEOF());
    assertFalse(lag.isEOF());

    // Lag still holding on to the last batch.
    assertEquals(1, buffer.size());
  }

  @Test
  public void testLag1Lead1ManyBatchsLoadOnDemand()
  {
    final int rowCount = 100;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    WindowFrameCursor primary = new PrimaryCursor(buffer);
    WindowFrameCursor lag = new LagCursor(buffer, 1);
    WindowFrameCursor lead = new LeadCursor(buffer, 1);
    WindowFrameSequencer seq = new WindowFrameSequencer(
        buffer,
        primary,
        Arrays.asList(lag, lead)
    );
    startSequencer(seq);
    assertFalse(primary.isEOF());
    assertFalse(primary.isValid());
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < 1) {
        assertTrue(lag.columns().scalar(0).isNull());
      } else {
        assertEquals(i - 0L, lag.columns().scalar(0).getLong());
      }
      if (i < rowCount - 1) {
        assertEquals(i + 2L, lead.columns().scalar(0).getLong());
      } else {
        assertTrue(lead.columns().scalar(0).isNull());
      }
    }
    assertFalse(seq.next());
    assertTrue(primary.isEOF());
    assertTrue(lead.isEOF());
    assertFalse(lag.isEOF());

    // Lag still holding on to the last batch.
    assertEquals(1, buffer.size());
  }

  @Test
  public void testLagLeadThreeBatchesLoadOnDemand()
  {
    final int rowCount = 100;
    BatchOperator op = dataGen(5, rowCount);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    WindowFrameCursor primary = new PrimaryCursor(buffer);
    WindowFrameCursor lag = new LagCursor(buffer, 15);
    WindowFrameCursor lead = new LeadCursor(buffer, 15);
    WindowFrameSequencer seq = new WindowFrameSequencer(
        buffer,
        primary,
        Arrays.asList(lag, lead)
    );
    startSequencer(seq);
    assertFalse(primary.isEOF());
    assertFalse(primary.isValid());
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < 15) {
        assertTrue(lag.columns().scalar(0).isNull());
      } else {
        assertEquals(i - 14L, lag.columns().scalar(0).getLong());
      }
      if (i < rowCount - 15) {
        assertEquals(i + 16L, lead.columns().scalar(0).getLong());
      } else {
        assertTrue(lead.columns().scalar(0).isNull());
      }
    }
    assertFalse(seq.next());
    assertTrue(primary.isEOF());
    assertTrue(lead.isEOF());
    assertFalse(lag.isEOF());

    // Lag still holding on to the tail batches.
    assertEquals(3, buffer.size());
  }
}
