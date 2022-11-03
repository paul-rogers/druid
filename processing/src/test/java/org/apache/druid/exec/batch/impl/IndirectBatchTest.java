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

package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.shim.ObjectArrayListBatchType;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IndirectBatchTest
{
  @Test
  public void testBatchType()
  {
    BatchType batchType = IndirectBatchType.of(ObjectArrayListBatchType.INSTANCE);
    assertEquals(BatchFormat.OBJECT_ARRAY, batchType.format());
    assertTrue(batchType.canSeek());
    assertFalse(batchType.canSort());
    assertFalse(batchType.canDirectCopyFrom(batchType));
    assertEquals(0, batchType.sizeOf(null));
    assertEquals(0, batchType.sizeOf(IndirectBatchType.wrap(null, new int[] {})));
    assertEquals(1, batchType.sizeOf(
        IndirectBatchType.wrap(
            Collections.singletonList(new Object[] {}),
            new int[] {0})
        )
    );
  }

  @Test
  public void testEmpty()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch base = BatchBuilder.arrayList(schema).build();
    Batch indirectBatch = Batches.indirectBatch(base, new int[] {});
    BatchCursor cursor = indirectBatch.newCursor();
    assertEquals(schema, cursor.columns().schema());
    assertFalse(cursor.positioner().next());
  }

  @Test
  public void testReorder()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch base = BatchBuilder.arrayList(schema)
        .row("first", 1)
        .row("second", 2)
        .row("third", 3)
        .row("fourth", 4)
        .row("fifth", 5)
        .build();
    Batch indirectBatch = Batches.indirectBatch(base, new int[] {2, 4, 1, 3, 0});

    Batch expected = BatchBuilder.arrayList(schema)
        .row("third", 3)
        .row("fifth", 5)
        .row("second", 2)
        .row("fourth", 4)
        .row("first", 1)
        .build();
    BatchValidator.assertEquals(expected, indirectBatch);
  }

  @Test
  public void testSelection()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch base = BatchBuilder.arrayList(schema)
        .row("first", 1)
        .row("second", 2)
        .row("third", 3)
        .row("fourth", 4)
        .row("fifth", 5)
        .build();
    Batch indirectBatch = Batches.indirectBatch(base, new int[] {1, 3});

    Batch expected = BatchBuilder.arrayList(schema)
        .row("second", 2)
        .row("fourth", 4)
        .build();
    BatchValidator.assertEquals(expected, indirectBatch);
  }
}
