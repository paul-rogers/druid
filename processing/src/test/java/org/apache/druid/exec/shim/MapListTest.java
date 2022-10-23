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

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchCapabilities;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.ColumnReaderFactory;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.operator.BatchCapabilities.BatchFormat;
import org.apache.druid.exec.operator.BatchReader.BatchCursor;
import org.apache.druid.exec.operator.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.operator.impl.Batches;
import org.apache.druid.exec.util.BatchBuilder;
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
 * No need to test the cursor in detail: that is already done in
 * {@link SeekableCursorTest}.
 */
public class MapListTest
{
  @Test
  public void testCapabilities()
  {
    MapListBatch batch = new MapListBatch(Batches.emptySchema(), Collections.emptyList());
    BatchCapabilities cap = batch.capabilities();
    assertEquals(BatchFormat.MAP, cap.format());
    assertTrue(cap.canSeek());
    assertFalse(cap.canSort());
  }

  @Test
  public void testEmptySchema()
  {
    RowSchema schema = new SchemaBuilder().build();
    Batch batch = BatchBuilder.mapList(schema).build();
    BatchReader reader = batch.newReader();

    assertSame(schema, reader.columns().schema());
    ColumnReaderFactory rowReader = reader.columns();
    assertSame(schema, rowReader.schema());

    BatchCursor cursor = reader.cursor();
    assertEquals(0, cursor.size());
  }

  @Test
  public void testEmptyBatch()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch batch = BatchBuilder.mapList(schema).build();
    BatchReader reader = batch.newReader();

    // Can obtain column readers but nothing to read
    ColumnReaderFactory rowReader = reader.columns();
    assertSame(schema, rowReader.schema());
    assertSame(schema.column(0), rowReader.scalar(0).schema());
    assertSame(schema.column("b"), rowReader.scalar("b").schema());

    BatchCursor cursor = reader.cursor();
    assertEquals(0, cursor.size());
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
    BatchReader reader = batch.newReader();

    assertSame(schema, reader.columns().schema());
    ColumnReaderFactory rowReader = reader.columns();
    assertSame(schema, rowReader.schema());

    BatchCursor cursor = reader.cursor();
    assertEquals(2, cursor.size());
    assertEquals(-1, cursor.index());
    assertTrue(cursor.next());
    assertEquals(0, cursor.index());
    assertEquals("first", rowReader.scalar(0).getString());
    assertTrue(cursor.next());
    assertEquals(1, cursor.index());
    assertEquals("second", rowReader.scalar(0).getString());
    assertFalse(cursor.next());
    assertEquals(2, cursor.index());

    // OK to move next at EOF, stays at EOF.
    assertFalse(cursor.next());
    assertEquals(2, cursor.index());

    cursor.reset();
    assertEquals(-1, cursor.index());

    assertTrue(cursor.seek(1));
    assertEquals(1, cursor.index());
    assertEquals("second", rowReader.scalar(0).getString());
    assertTrue(cursor.seek(0));
    assertEquals(0, cursor.index());
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
    BatchReader reader = batch.newReader();
    ColumnReaderFactory rowReader = reader.columns();

    BatchCursor cursor = reader.cursor();
    assertTrue(cursor.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertEquals(1L, rowReader.scalar(1).getLong());
    assertTrue(cursor.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertEquals(2L, rowReader.scalar(1).getLong());
    assertFalse(cursor.next());
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
    BatchReader reader = batch.newReader();
    ColumnReaderFactory rowReader = reader.columns();

    BatchCursor cursor = reader.cursor();
    assertTrue(cursor.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertEquals(1L, rowReader.scalar(1).getLong());
    assertTrue(cursor.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertEquals(2L, rowReader.scalar(1).getLong());
    assertFalse(cursor.next());

    // Second batch
    batch = batchBuilder
        .newBatch()
        .row("third", 3)
        .row("fourth", 4)
        .build();
    BatchReader reader2 = batch.bindReader(reader);
    assertSame(reader2, reader);

    assertTrue(cursor.next());
    assertEquals("third", rowReader.scalar(0).getString());
    assertEquals(3L, rowReader.scalar(1).getLong());
    assertTrue(cursor.next());
    assertEquals("fourth", rowReader.scalar(0).getString());
    assertEquals(4L, rowReader.scalar(1).getLong());
    assertFalse(cursor.next());
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
    BatchReader reader = batch.newReader();

    // Reader does actual reading.
    ColumnReaderFactory rowReader = reader.columns();
    ScalarColumnReader lReader = rowReader.scalar(0);
    ScalarColumnReader sReader = rowReader.scalar("s");
    ScalarColumnReader fReader = rowReader.scalar(2);
    ScalarColumnReader dReader = rowReader.scalar("d");
    ScalarColumnReader oReader = rowReader.scalar(4);
    ScalarColumnReader nReader = rowReader.scalar("n");

    BatchCursor cursor = reader.cursor();
    assertTrue(cursor.next());
    assertFalse(lReader.isNull());
    assertEquals(1L, lReader.getLong());
    assertTrue(sReader.isNull());
    assertTrue(nReader.isNull());

    assertTrue(cursor.next());
    assertFalse(lReader.isNull());
    assertEquals(2L, lReader.getLong());
    assertFalse(sReader.isNull());
    assertEquals("second", sReader.getString());
    assertEquals(10D, fReader.getDouble(), 0.000001);
    assertEquals(20D, dReader.getDouble(), 0.000001);
    assertTrue(oReader.getObject() instanceof Object);
    assertTrue(nReader.isNull());

    assertFalse(cursor.next());
  }
}
