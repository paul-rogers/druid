package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.window.PartitionSequencer.LagSequencer;
import org.apache.druid.exec.window.PartitionSequencer.LeadSequencer;
import org.apache.druid.exec.window.PartitionSequencer.PrimarySequencer;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PartitionSequencerTest
{
  private BatchBuffer buildBuffer(int batchSize, int rowCount)
  {
    BatchOperator op = WindowTests.dataGen(batchSize, rowCount);
    return new BatchBuffer(op.batchSchema(), op.open());
  }

  private BatchBuffer preloadBuffer(int batchSize, int rowCount)
  {
    BatchBuffer buffer = buildBuffer(batchSize, rowCount);
    for (int i = 0; ; i++) {
      if (buffer.loadBatch(i) == null) {
        break;
      }
    }
    return buffer;
  }

  /**
   * Basic sanity test for the empty-input edge case.
   */
  @Test
  public void testPrimaryEmptyPreloaded()
  {
    BatchBuffer buffer = preloadBuffer(5, 0);
    BatchReader reader = buffer.inputSchema.newReader();
    PartitionSequencer seq = new PrimarySequencer(buffer, reader);
    seq.bindBatchListener(false, false);
    seq.startPartition();
    assertFalse(seq.next());
    assertTrue(seq.isEOF());
  }

  /**
   * One batch (to bypass the multi-batch logic), preloaded in
   * the buffer (to bypass the auto-loading logic). Simulates a
   * small result set when aggregation has pre-loaded the buffer.
   */
  @Test
  public void testPrimaryPreloadedOneBatch()
  {
    final int rowCount = 4;
    BatchBuffer buffer = preloadBuffer(5, rowCount);
    BatchReader reader = buffer.inputSchema.newReader();
    PartitionSequencer seq = new PrimarySequencer(buffer, reader);
    seq.bindBatchListener(false, false);
    seq.startPartition();
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, reader.columns().scalar(0).getLong());
    }
    assertFalse(seq.next());
    assertTrue(seq.isEOF());
  }

  /**
   * Edge case: EOF at the exact max size of a batch.
   */
  @Test
  public void testPrimaryPreloadedOneFullBatch()
  {
    final int rowCount = 5;
    BatchBuffer buffer = preloadBuffer(5, rowCount);
    BatchReader reader = buffer.inputSchema.newReader();
    PartitionSequencer seq = new PrimarySequencer(buffer, reader);
    seq.bindBatchListener(false, false);
    seq.startPartition();
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, reader.columns().scalar(0).getLong());
    }
    assertFalse(seq.next());
    assertTrue(seq.isEOF());
  }

  /**
   * Test of the multi-batch logic, but without the auto-load
   * parts.
   */
  @Test
  public void testPrimaryPreloadedManyBatches()
  {
    final int rowCount = 24;
    BatchBuffer buffer = preloadBuffer(5, rowCount);
    BatchReader reader = buffer.inputSchema.newReader();
    PartitionSequencer seq = new PrimarySequencer(buffer, reader);
    seq.bindBatchListener(false, false);
    seq.startPartition();
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, reader.columns().scalar(0).getLong());
    }
    assertFalse(seq.next());
    assertTrue(seq.isEOF());
  }

  /**
   * Test streaming (auto-load) of one batch of data.
   */
  @Test
  public void testPrimaryOneBatchStreaming()
  {
    final int rowCount = 4;
    BatchBuffer buffer = buildBuffer(5, rowCount);
    BatchReader reader = buffer.inputSchema.newReader();
    PartitionSequencer seq = new PrimarySequencer(buffer, reader);
    seq.bindBatchListener(true, true);
    seq.startPartition();
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, reader.columns().scalar(0).getLong());
    }
    assertFalse(seq.next());
    assertTrue(seq.isEOF());
    assertEquals(0, buffer.size());
  }

  /**
   * Test streaming (auto-load) of multiple batches of data.
   * Tests both the multi-batch and the multi-batch load logic.
   */
  @Test
  public void testPrimaryManyBatchesStreaming()
  {
    final int rowCount = 24;
    BatchBuffer buffer = buildBuffer(5, rowCount);
    BatchReader reader = buffer.inputSchema.newReader();
    PartitionSequencer seq = new PrimarySequencer(buffer, reader);
    seq.bindBatchListener(true, true);
    seq.startPartition();
    for (int i = 0; i < rowCount; i++) {
      assertTrue(seq.next());
      assertEquals(i + 1L, reader.columns().scalar(0).getLong());
    }
    assertFalse(seq.next());
    assertTrue(seq.isEOF());
    assertEquals(0, buffer.size());
  }

  /**
   * Add a lag reader, offset by 2 from the primary reader. Tests
   * the lag logic in the simple case.
   */
  @Test
  public void testLag2TwoBatchsStreaming()
  {
    final int rowCount = 9;
    BatchBuffer buffer = buildBuffer(5, rowCount);
    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchListener(true, false);
    BatchReader lag = buffer.inputSchema.newReader();
    PartitionSequencer lagSeq = new LagSequencer(buffer, lag, 2);
    lagSeq.bindBatchListener(false, true);

    primarySeq.startPartition();
    lagSeq.startPartition();
    assertFalse(primarySeq.isEOF());
    assertFalse(lagSeq.isEOF());
    for (int i = 0; i < rowCount; i++) {
      // The primary reader is advanced first since it loads
      // batches in this case.
      assertTrue(primarySeq.next());
      lagSeq.next();
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < 2) {
        assertTrue(lag.columns().scalar(0).isNull());
      } else {
        assertEquals(i - 1L, lag.columns().scalar(0).getLong());
      }
    }
    assertFalse(primarySeq.next());
    assertTrue(primarySeq.isEOF());
    assertFalse(lagSeq.isEOF());

    // Lag still holding on to the last batch.
    assertEquals(1, buffer.size());
  }

  /**
   * Add a lead reader, offset by 2 from the primary reader. Tests
   * the lead logic in the simple case.
   */
  @Test
  public void testLead2TwoBatchesStreaming()
  {
    final int rowCount = 9;
    BatchBuffer buffer = buildBuffer(5, rowCount);
    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchListener(false, true);
    BatchReader lead = buffer.inputSchema.newReader();
    PartitionSequencer leadSeq = new LeadSequencer(buffer, lead, 2);
    leadSeq.bindBatchListener(true, false);

    primarySeq.startPartition();
    leadSeq.startPartition();
    assertFalse(primarySeq.isEOF());
    assertFalse(leadSeq.isEOF());
    for (int i = 0; i < rowCount; i++) {
      // The lead reader must be moved first, since it does
      // the batch loading. The primary is still used to detect
      // EOF.
      leadSeq.next();
      assertTrue(primarySeq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < rowCount - 2) {
        assertEquals(i + 3L, lead.columns().scalar(0).getLong());
      } else {
        assertTrue(lead.columns().scalar(0).isNull());
      }
    }
    assertFalse(primarySeq.next());
    assertTrue(primarySeq.isEOF());
    assertTrue(leadSeq.isEOF());

    // Primary released all the batches as it moved past them.
    assertEquals(0, buffer.size());
  }

  /**
   * Both lead and lag, along with the primary reader.
   */
  @Test
  public void testLag2Lead2TwoBatchsStreaming()
  {
    final int rowCount = 9;
    BatchBuffer buffer = buildBuffer(5, rowCount);

    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchListener(false, false);

    BatchReader lead = buffer.inputSchema.newReader();
    PartitionSequencer leadSeq = new LeadSequencer(buffer, lead, 2);
    leadSeq.bindBatchListener(true, false);

    BatchReader lag = buffer.inputSchema.newReader();
    PartitionSequencer lagSeq = new LagSequencer(buffer, lag, 2);
    lagSeq.bindBatchListener(false, true);

    primarySeq.startPartition();
    leadSeq.startPartition();
    lagSeq.startPartition();

    assertFalse(primarySeq.isEOF());

    for (int i = 0; i < rowCount; i++) {
      // Ordering is important: lead-most first, lag-most last
      leadSeq.next();
      assertTrue(primarySeq.next());
      lagSeq.next();

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
    assertFalse(leadSeq.next());
    assertTrue(leadSeq.isEOF());
    assertTrue(leadSeq.isEOF());
    assertFalse(lagSeq.isEOF());

    // Lag still holding on to the last batch.
    assertEquals(1, buffer.size());
  }

  /**
   * Extreme case: lag is greater than the actual row count: all
   * values are null.
   */
  @Test
  public void testExtremeLagStreaming()
  {
    final int batchSize = 5;
    final int batchCount = 4;
    final int rowCount = batchSize * batchCount;
    BatchBuffer buffer = buildBuffer(batchSize, rowCount);
    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchListener(true, false);
    BatchReader lag = buffer.inputSchema.newReader();
    PartitionSequencer lagSeq = new LagSequencer(buffer, lag, 2 * rowCount);
    lagSeq.bindBatchListener(false, true);

    primarySeq.startPartition();
    lagSeq.startPartition();
    assertFalse(primarySeq.isEOF());
    assertFalse(lagSeq.isEOF());
    for (int i = 0; i < rowCount; i++) {
      // The primary reader is advanced first since it loads
      // batches in this case.
      assertTrue(primarySeq.next());
      lagSeq.next();
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      assertTrue(lag.columns().scalar(0).isNull());
    }
    assertFalse(primarySeq.next());
    assertTrue(primarySeq.isEOF());
    assertFalse(lagSeq.isEOF());

    // Lag still holding on to all batches.
    assertEquals(batchCount, buffer.size());
  }

  /**
   * Extreme case: lead is greater than the actual row count: all
   * values are null.
   */
  @Test
  public void testExtremeLeadStreaming()
  {
    final int rowCount = 9;
    BatchBuffer buffer = buildBuffer(5, rowCount);
    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchListener(false, true);
    BatchReader lead = buffer.inputSchema.newReader();
    PartitionSequencer leadSeq = new LeadSequencer(buffer, lead, 2 * rowCount);
    leadSeq.bindBatchListener(true, false);

    primarySeq.startPartition();
    leadSeq.startPartition();
    assertFalse(primarySeq.isEOF());
    assertTrue(leadSeq.isEOF());
    for (int i = 0; i < rowCount; i++) {
      // The lead reader must be moved first, since it does
      // the batch loading. The primary is still used to detect
      // EOF.
      leadSeq.next();
      assertTrue(primarySeq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      assertTrue(lead.columns().scalar(0).isNull());
    }
    assertFalse(primarySeq.next());
    assertTrue(primarySeq.isEOF());
    assertTrue(leadSeq.isEOF());

    // Primary released all the batches as it moved past them.
    assertEquals(0, buffer.size());
  }
}
