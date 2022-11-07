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

import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.test.EmptyBatchOperator;
import org.apache.druid.exec.test.SimpleDataGenOperator;
import org.apache.druid.exec.test.SimpleDataGenSpec;
import org.apache.druid.exec.test.TestUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class BatchBufferTest
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

  @Test
  public void testEmptyInput()
  {
    BatchOperator op = dataGen(5, 0);
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());
    assertFalse(buffer.isEof());
    assertNull(buffer.loadBatch(0));
    assertTrue(buffer.isEof());
  }

  @Test
  public void testOneInputBatch()
  {
    BatchOperator op = dataGen(5, 1);
    BatchType batchType = op.batchSchema().type();
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());

    assertFalse(buffer.isEof());
    assertNotNull(buffer.loadBatch(0));
    assertFalse(buffer.isEof());
    assertEquals(1, batchType.sizeOf(buffer.batch(0)));

    assertNull(buffer.loadBatch(1));
    assertTrue(buffer.isEof());

    // Should never happen. Test stability to small errors.
    assertNull(buffer.loadBatch(1));
    assertTrue(buffer.isEof());
  }

  @Test
  public void testMultipleInputBatches()
  {
    testThreeBatches(dataGen(5, 14));
  }

  private void testThreeBatches(BatchOperator op)
  {
    BatchType batchType = op.batchSchema().type();
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());

    assertNotNull(buffer.loadBatch(0));
    assertEquals(5, batchType.sizeOf(buffer.batch(0)));

    assertNotNull(buffer.loadBatch(1));
    assertEquals(5, batchType.sizeOf(buffer.batch(0)));
    assertEquals(5, batchType.sizeOf(buffer.batch(1)));

    assertNotNull(buffer.loadBatch(2));
    assertEquals(5, batchType.sizeOf(buffer.batch(0)));
    assertEquals(5, batchType.sizeOf(buffer.batch(1)));
    assertEquals(4, batchType.sizeOf(buffer.batch(2)));

    assertNull(buffer.loadBatch(3));
    assertTrue(buffer.isEof());
  }

  @Test
  public void testEmptyInputBatches()
  {
    FragmentContext context = TestUtils.emptyFragment();
    BatchOperator op = new SimpleDataGenOperator(
        context,
        dataGenSpec(5, 14)
    );
    op = new EmptyBatchOperator(context, op);
    testThreeBatches(op);
  }

  @Test
  public void testSlidingWindow()
  {
    BatchOperator op = dataGen(5, 14);
    BatchType batchType = op.batchSchema().type();
    BatchBuffer buffer = new BatchBuffer(op.batchSchema(), op.open());

    assertNotNull(buffer.loadBatch(0));
    assertEquals(5, batchType.sizeOf(buffer.batch(0)));

    // Unload a batch no longer needed. Should now be unavailable.
    buffer.unloadBatches(0, 0);
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.batch(0));

    assertNotNull(buffer.loadBatch(1));
    assertEquals(5, batchType.sizeOf(buffer.batch(1)));
    buffer.unloadBatches(1, 1);
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.batch(1));

    assertNotNull(buffer.loadBatch(2));
    assertEquals(4, batchType.sizeOf(buffer.batch(2)));
    buffer.unloadBatches(2, 2);
    assertThrows(IndexOutOfBoundsException.class, () -> buffer.batch(2));

    assertNull(buffer.loadBatch(3));
    assertTrue(buffer.isEof());
  }
}
