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

  private static final BatchUnloader NO_OP_UNLOADER = b -> { };

  /**
   * Basic sanity test for the empty-input edge case.
   */
  @Test
  public void testPrimaryEmptyPreloaded()
  {
    BatchBuffer buffer = preloadBuffer(5, 0);
    BatchReader reader = buffer.inputSchema.newReader();
    PartitionSequencer seq = new PrimarySequencer(buffer, reader);
    seq.bindBatchLoader(buffer);
    seq.bindBatchUnloader(NO_OP_UNLOADER);
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
    seq.bindBatchLoader(buffer);
    seq.bindBatchUnloader(NO_OP_UNLOADER);
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
    seq.bindBatchLoader(buffer);
    seq.bindBatchUnloader(NO_OP_UNLOADER);
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
    seq.bindBatchLoader(buffer);
    seq.bindBatchUnloader(NO_OP_UNLOADER);
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
    seq.bindBatchLoader(buffer);
    seq.bindBatchUnloader(buffer);
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
    seq.bindBatchLoader(buffer);
    seq.bindBatchUnloader(buffer);
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
  public void testLagTwoBatchesStreaming()
  {
    final int rowCount = 9;
    BatchBuffer buffer = buildBuffer(5, rowCount);

    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchLoader(buffer);
    primarySeq.bindBatchUnloader(NO_OP_UNLOADER);

    BatchReader lag = buffer.inputSchema.newReader();
    PartitionSequencer lagSeq = new LagSequencer(buffer, lag, 2);
    lagSeq.bindBatchLoader(buffer);
    lagSeq.bindBatchUnloader(buffer);

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

  @Test
  public void testTwoLagsTwoBatchesStreaming()
  {
    final int rowCount = 9;
    BatchBuffer buffer = buildBuffer(5, rowCount);

    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchLoader(buffer);
    primarySeq.bindBatchUnloader(NO_OP_UNLOADER);

    BatchReader lag1 = buffer.inputSchema.newReader();
    PartitionSequencer lag1Seq = new LagSequencer(buffer, lag1, 2);
    lag1Seq.bindBatchLoader(buffer);
    lag1Seq.bindBatchUnloader(NO_OP_UNLOADER);

    BatchReader lag2 = buffer.inputSchema.newReader();
    PartitionSequencer lag2Seq = new LagSequencer(buffer, lag2, 4);
    lag2Seq.bindBatchLoader(buffer);
    lag2Seq.bindBatchUnloader(buffer);

    primarySeq.startPartition();
    lag1Seq.startPartition();
    lag2Seq.startPartition();
    assertFalse(primarySeq.isEOF());
    assertFalse(lag1Seq.isEOF());
    assertFalse(lag2Seq.isEOF());
    for (int i = 0; i < rowCount; i++) {
      // The primary reader is advanced first since it loads
      // batches in this case.
      assertTrue(primarySeq.next());
      lag1Seq.next();

      // The tail-most lag is advanced last as it unloads batches
      lag2Seq.next();
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < 2) {
        assertTrue(lag1.columns().scalar(0).isNull());
      } else {
        assertEquals(i - 1L, lag1.columns().scalar(0).getLong());
      }
      if (i < 4) {
        assertTrue(lag2.columns().scalar(0).isNull());
      } else {
        assertEquals(i - 3L, lag2.columns().scalar(0).getLong());
      }
    }
    assertFalse(primarySeq.next());
    assertTrue(primarySeq.isEOF());
    assertFalse(lag1Seq.isEOF());
    assertFalse(lag2Seq.isEOF());

    // Lags still holding on to the last two batches.
    assertEquals(2, buffer.size());
  }

  /**
   * Add a lead reader, offset by 2 from the primary reader. Tests
   * the lead logic in the simple case.
   */
  @Test
  public void testLeadTwoBatchesStreaming()
  {
    final int rowCount = 9;
    BatchBuffer buffer = buildBuffer(5, rowCount);

    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchLoader(buffer);
    primarySeq.bindBatchUnloader(buffer);

    BatchReader lead = buffer.inputSchema.newReader();
    PartitionSequencer leadSeq = new LeadSequencer(buffer, lead, 2);
    leadSeq.bindBatchLoader(buffer);
    leadSeq.bindBatchUnloader(NO_OP_UNLOADER);

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

  @Test
  public void testTwoLeadsTwoBatchesStreaming()
  {
    final int rowCount = 9;
    BatchBuffer buffer = buildBuffer(5, rowCount);

    BatchReader primary = buffer.inputSchema.newReader();
    PartitionSequencer primarySeq = new PrimarySequencer(buffer, primary);
    primarySeq.bindBatchLoader(buffer);
    primarySeq.bindBatchUnloader(buffer);

    BatchReader lead1 = buffer.inputSchema.newReader();
    PartitionSequencer lead1Seq = new LeadSequencer(buffer, lead1, 2);
    lead1Seq.bindBatchLoader(buffer);
    lead1Seq.bindBatchUnloader(NO_OP_UNLOADER);

    BatchReader lead2 = buffer.inputSchema.newReader();
    PartitionSequencer lead2Seq = new LeadSequencer(buffer, lead2, 4);
    lead2Seq.bindBatchLoader(buffer);
    lead2Seq.bindBatchUnloader(NO_OP_UNLOADER);

    primarySeq.startPartition();
    lead1Seq.startPartition();
    lead2Seq.startPartition();
    assertFalse(primarySeq.isEOF());
    assertFalse(lead1Seq.isEOF());
    assertFalse(lead2Seq.isEOF());
    for (int i = 0; i < rowCount; i++) {
      // The lead 4 reader must be moved first, since it does
      // the batch loading. The primary is still used to detect
      // EOF.
      lead2Seq.next();
      lead1Seq.next();
      assertTrue(primarySeq.next());
      assertEquals(i + 1L, primary.columns().scalar(0).getLong());
      if (i < rowCount - 2) {
        assertEquals(i + 3L, lead1.columns().scalar(0).getLong());
      } else {
        assertTrue(lead1.columns().scalar(0).isNull());
      }
      if (i < rowCount - 4) {
        assertEquals(i + 5L, lead2.columns().scalar(0).getLong());
      } else {
        assertTrue(lead2.columns().scalar(0).isNull());
      }
    }
    assertFalse(primarySeq.next());
    assertTrue(primarySeq.isEOF());
    assertTrue(lead1Seq.isEOF());
    assertTrue(lead2Seq.isEOF());

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
    primarySeq.bindBatchLoader(buffer);
    primarySeq.bindBatchUnloader(NO_OP_UNLOADER);

    BatchReader lead = buffer.inputSchema.newReader();
    PartitionSequencer leadSeq = new LeadSequencer(buffer, lead, 2);
    leadSeq.bindBatchLoader(buffer);
    leadSeq.bindBatchUnloader(NO_OP_UNLOADER);

    BatchReader lag = buffer.inputSchema.newReader();
    PartitionSequencer lagSeq = new LagSequencer(buffer, lag, 2);
    lagSeq.bindBatchLoader(buffer);
    lagSeq.bindBatchUnloader(buffer);

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
    primarySeq.bindBatchLoader(buffer);
    primarySeq.bindBatchUnloader(NO_OP_UNLOADER);

    BatchReader lag = buffer.inputSchema.newReader();
    PartitionSequencer lagSeq = new LagSequencer(buffer, lag, 2 * rowCount);
    lagSeq.bindBatchLoader(buffer);
    lagSeq.bindBatchUnloader(buffer);

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
    primarySeq.bindBatchLoader(buffer);
    primarySeq.bindBatchUnloader(buffer);

    BatchReader lead = buffer.inputSchema.newReader();
    PartitionSequencer leadSeq = new LeadSequencer(buffer, lead, 2 * rowCount);
    leadSeq.bindBatchLoader(buffer);
    leadSeq.bindBatchUnloader(NO_OP_UNLOADER);

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
