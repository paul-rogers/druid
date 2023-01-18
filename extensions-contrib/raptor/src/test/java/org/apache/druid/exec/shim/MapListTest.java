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

package org.apache.druid.exec.shim;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchPositioner;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.impl.SimpleBatchPositionerTest;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test the object array batch, which tests many of the component
 * pieces. Note: we do not use the batch validator here because
 * the batch validator requires this code to work.
 * <p>
 * No need to test the positioner in detail: that is already done in
 * {@link SimpleBatchPositionerTest}.
 */
public class MapListTest
{
  @Test
  public void testCapabilities()
  {
    BatchType batchType = MapListBatchType.INSTANCE;
    assertEquals(BatchFormat.MAP, batchType.format());
    assertTrue(batchType.canSeek());
    assertFalse(batchType.canSort());
    assertTrue(batchType.canWrite());
    assertEquals(0, batchType.sizeOf(null));
    assertEquals(0, batchType.sizeOf(Collections.emptyList()));
    assertEquals(1, batchType.sizeOf(Collections.singletonList(ImmutableMap.of("foo", "bar"))));
  }

  @Test
  public void testEmptySchema()
  {
    RowSchema schema = new SchemaBuilder().build();
    Batch batch = BatchBuilder.mapList(schema).build();
    BatchCursor cursor = batch.newCursor();

    assertSame(schema, cursor.columns().schema());
    ColumnReaderProvider rowReader = cursor.columns();
    assertSame(schema, rowReader.schema());

    BatchPositioner positioner = cursor.positioner();
    assertEquals(0, positioner.size());
  }

  @Test
  public void testEmptyBatch()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch batch = BatchBuilder.mapList(schema).build();
    BatchCursor cursor = batch.newCursor();

    // Can obtain column readers but nothing to read
    ColumnReaderProvider rowReader = cursor.columns();
    assertSame(schema, rowReader.schema());
    assertSame(schema.column(0), rowReader.scalar(0).schema());
    assertSame(schema.column("b"), rowReader.scalar("b").schema());
    assertTrue(rowReader.scalar(0).isNull());
    assertTrue(rowReader.scalar(1).isNull());

    BatchPositioner positioner = cursor.positioner();
    assertEquals(0, positioner.size());
  }

  @Test
  public void testSingleColumn()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .build();

    // Use singletonRow(), mostly in the case that the one
    // column is an array, but also works for scalars.
    Batch batch = BatchBuilder.mapList(schema)
        .singletonRow("first")
        .singletonRow("second")
        .build();
    BatchCursor cursor = batch.newCursor();

    assertSame(schema, cursor.columns().schema());
    ColumnReaderProvider rowReader = cursor.columns();
    assertSame(schema, rowReader.schema());

    BatchPositioner positioner = cursor.positioner();
    assertEquals(2, positioner.size());
    assertEquals(-1, positioner.index());
    assertTrue(positioner.next());
    assertEquals(0, positioner.index());
    assertEquals("first", rowReader.scalar(0).getString());
    assertTrue(positioner.next());
    assertEquals(1, positioner.index());
    assertEquals("second", rowReader.scalar(0).getString());
    assertFalse(positioner.next());
    assertEquals(2, positioner.index());
    assertTrue(rowReader.scalar(0).isNull());

    // OK to move next at EOF, stays at EOF.
    assertFalse(positioner.next());
    assertEquals(2, positioner.index());

    positioner.reset();
    assertEquals(-1, positioner.index());

    assertTrue(positioner.seek(1));
    assertEquals(1, positioner.index());
    assertEquals("second", rowReader.scalar(0).getString());
    assertTrue(positioner.seek(0));
    assertEquals(0, positioner.index());
    assertEquals("first", rowReader.scalar(0).getString());
  }

  /**
   * Test the typical case.
   */
  @Test
  public void testMultipleColumns()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();

    // Pass an int value for "b". Should map to the defined
    // type.
    Batch batch = BatchBuilder.mapList(schema)
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchCursor cursor = batch.newCursor();
    ColumnReaderProvider rowReader = cursor.columns();

    BatchPositioner positioner = cursor.positioner();
    assertTrue(positioner.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertEquals(1L, rowReader.scalar(1).getLong());
    assertTrue(positioner.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertEquals(2L, rowReader.scalar(1).getLong());
    assertFalse(positioner.next());
  }

  @Test
  public void testMultipleBatches()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();

    // First batch & setup
    BatchBuilder batchBuilder = BatchBuilder.mapList(schema);
    Batch batch = batchBuilder
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchCursor cursor = batch.newCursor();
    ColumnReaderProvider rowReader = cursor.columns();

    BatchPositioner positioner = cursor.positioner();
    assertTrue(positioner.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertEquals(1L, rowReader.scalar(1).getLong());
    assertTrue(positioner.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertEquals(2L, rowReader.scalar(1).getLong());
    assertFalse(positioner.next());

    // Second batch
    batch = batchBuilder
        .newBatch()
        .row("third", 3)
        .row("fourth", 4)
        .build();
    batch.bindCursor(cursor);

    assertTrue(positioner.next());
    assertEquals("third", rowReader.scalar(0).getString());
    assertEquals(3L, rowReader.scalar(1).getLong());
    assertTrue(positioner.next());
    assertEquals("fourth", rowReader.scalar(0).getString());
    assertEquals(4L, rowReader.scalar(1).getLong());
    assertFalse(positioner.next());
  }

  @Test
  public void testAllScalarTypes()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("l", ColumnType.LONG)
        .scalar("s", ColumnType.STRING)
        .scalar("f", ColumnType.FLOAT)
        .scalar("d", ColumnType.DOUBLE)
        .scalar("o", ColumnType.UNKNOWN_COMPLEX)
        .scalar("n", null)
        .build();
    Batch batch = BatchBuilder.mapList(schema)
        .row(1L, null, null, null, null, null)
        .row(2L, "second", 10f, 20D, new Object(), null)
        .build();
    BatchCursor cursor = batch.newCursor();

    // Reader does actual reading.
    ColumnReaderProvider rowReader = cursor.columns();
    ScalarColumnReader lReader = rowReader.scalar(0);
    ScalarColumnReader sReader = rowReader.scalar("s");
    ScalarColumnReader fReader = rowReader.scalar(2);
    ScalarColumnReader dReader = rowReader.scalar("d");
    ScalarColumnReader oReader = rowReader.scalar(4);
    ScalarColumnReader nReader = rowReader.scalar("n");

    BatchPositioner positioner = cursor.positioner();
    assertTrue(positioner.next());
    assertFalse(lReader.isNull());
    assertEquals(1L, lReader.getLong());
    assertTrue(sReader.isNull());
    assertTrue(nReader.isNull());

    assertTrue(positioner.next());
    assertFalse(lReader.isNull());
    assertEquals(2L, lReader.getLong());
    assertFalse(sReader.isNull());
    assertEquals("second", sReader.getString());
    assertEquals(10D, fReader.getDouble(), 0.000001);
    assertEquals(20D, dReader.getDouble(), 0.000001);
    assertTrue(oReader.getObject() instanceof Object);
    assertTrue(nReader.isNull());

    assertFalse(positioner.next());
  }
}
