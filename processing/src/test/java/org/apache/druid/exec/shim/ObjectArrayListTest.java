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

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchReader.BatchCursor;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.ColumnReaderFactory;
import org.apache.druid.exec.batch.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.impl.SeekableCursorTest;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

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
public class ObjectArrayListTest
{
  @Test
  public void testBatchType()
  {
    BatchType batchType = ObjectArrayListBatchType.INSTANCE;
    assertEquals(BatchFormat.OBJECT_ARRAY, batchType.format());
    assertTrue(batchType.canSeek());
    assertFalse(batchType.canSort());
    assertTrue(batchType.canDirectCopyFrom(batchType));
  }

  @Test
  public void testEmptySchema()
  {
    RowSchema schema = new SchemaBuilder().build();
    Batch batch = BatchBuilder.arrayList(schema).build();
    BatchReader reader = batch.newReader();

    assertSame(schema, reader.columns().schema());
    ColumnReaderFactory rowReader = reader.columns();
    assertSame(schema, rowReader.schema());

    BatchCursor cursor = reader.batchCursor();
    assertEquals(0, cursor.size());
  }

  @Test
  public void testEmptyBatch()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch batch = BatchBuilder.arrayList(schema).build();
    BatchReader reader = batch.newReader();

    // Can obtain column readers but nothing to read
    ColumnReaderFactory rowReader = reader.columns();
    assertSame(schema, rowReader.schema());
    assertSame(schema.column(0), rowReader.scalar(0).schema());
    assertSame(schema.column("b"), rowReader.scalar("b").schema());

    BatchCursor cursor = reader.batchCursor();
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
    Batch batch = BatchBuilder.arrayList(schema)
        .singletonRow("first")
        .singletonRow("second")
        .build();
    BatchReader reader = batch.newReader();

    assertSame(schema, reader.columns().schema());
    ColumnReaderFactory rowReader = reader.columns();
    assertSame(schema, rowReader.schema());

    BatchCursor cursor = reader.batchCursor();
    assertEquals(2, cursor.size());
    assertTrue(cursor.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertTrue(cursor.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertFalse(cursor.next());
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
    Batch batch = BatchBuilder.arrayList(schema)
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader reader = batch.newReader();
    ColumnReaderFactory rowReader = reader.columns();

    BatchCursor cursor = reader.batchCursor();
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
    BatchBuilder batchBuilder = BatchBuilder.arrayList(schema);
    Batch batch = batchBuilder
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader reader = batch.newReader();
    ColumnReaderFactory rowReader = reader.columns();

    BatchCursor cursor = reader.batchCursor();
    assertTrue(cursor.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertEquals(1L, rowReader.scalar(1).getLong());
    assertTrue(cursor.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertEquals(2L, rowReader.scalar(1).getLong());
    assertFalse(cursor.next());

    // Second batch
    batchBuilder.newBatch();
    batch = batchBuilder
        .row("third", 3)
        .row("fourth", 4)
        .row("fifth", 5)
        .build();
    batch.bindReader(reader);

    assertEquals(3, cursor.size());
    assertTrue(cursor.next());
    assertEquals("third", rowReader.scalar(0).getString());
    assertEquals(3L, rowReader.scalar(1).getLong());
    assertTrue(cursor.next());
    assertEquals("fourth", rowReader.scalar(0).getString());
    assertEquals(4L, rowReader.scalar(1).getLong());
    assertTrue(cursor.next());
    assertEquals("fifth", rowReader.scalar(0).getString());
    assertEquals(5L, rowReader.scalar(1).getLong());
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
    Batch batch = BatchBuilder.arrayList(schema)
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

    BatchCursor cursor = reader.batchCursor();
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

  /**
   * Test list direct copy. Covers all list-based formats since the
   * code is shared.
   */
  @Test
  public void testDirectCopy()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();

    ObjectArrayListWriter writer = new ObjectArrayListWriter(schema, 1000);
    writer.newBatch();

    BatchBuilder batchBuilder = BatchBuilder.arrayList(schema);
    Batch batch = batchBuilder
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader reader = batch.newReader();

    assertTrue(Batches.canDirectCopy(reader, writer));
    writer.directCopy(reader, 10);
    assertTrue(reader.cursor().isEOF());

    batchBuilder.newBatch();
    batch = batchBuilder
        .row("third", 3)
        .row("fourth", 4)
        .row("fifth", 5)
        .build();
    batch.bindReader(reader);

    writer.directCopy(reader, 1);
    assertEquals(0, reader.batchCursor().index());
    writer.directCopy(reader, 10);
    assertTrue(reader.cursor().isEOF());

    batchBuilder.newBatch();
    Batch expected = batchBuilder
          .row("first", 1)
          .row("second", 2)
          .row("third", 3)
          .row("fourth", 4)
          .row("fifth", 5)
          .build();

    BatchValidator.assertEquals(expected, writer.harvestAsBatch());
  }

  @Test
  public void testDirectCopyLimits()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();

    ObjectArrayListWriter writer = new ObjectArrayListWriter(schema, 4);
    writer.newBatch();

    BatchBuilder batchBuilder = BatchBuilder.arrayList(schema);
    Batch batch = batchBuilder
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader reader = batch.newReader();
    writer.directCopy(reader, 10);
    assertTrue(reader.cursor().isEOF());

    // Cursor is at EOF, try to copy again.
    writer.directCopy(reader, 10);
    assertTrue(reader.cursor().isEOF());

    // Copy more than fits into the batch
    batchBuilder.newBatch();
    batch = batchBuilder
        .row("third", 3)
        .row("fourth", 4)
        .row("fifth", 5)
        .build();
    batch.bindReader(reader);
    writer.directCopy(reader, 10);
    assertFalse(reader.cursor().isEOF());
    assertEquals(1, reader.batchCursor().index());

    // Stubbornly try again
    writer.directCopy(reader, 10);
    assertFalse(reader.cursor().isEOF());
    assertEquals(1, reader.batchCursor().index());

    batchBuilder.newBatch();
    Batch expected = batchBuilder
          .row("first", 1)
          .row("second", 2)
          .row("third", 3)
          .row("fourth", 4)
          .build();
    BatchValidator.assertEquals(expected, writer.harvestAsBatch());
  }
}
