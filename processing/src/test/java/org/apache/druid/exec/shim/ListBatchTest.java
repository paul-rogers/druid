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
import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchCursor.RowPositioner;
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class ListBatchTest
{
  @Test
  public void testEmptySchema()
  {
    RowSchema schema = new SchemaBuilder().build();
    verify(schema, ListBatchTest::verifyEmptySchema);
  }

  private void verify(RowSchema schema, Consumer<BatchBuilder> verifyFn)
  {
    verifyFn.accept(BatchBuilder.arrayList(schema));
    verifyFn.accept(BatchBuilder.mapList(schema));
    verifyFn.accept(BatchBuilder.scanResultValue("dummy", schema, ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST));
    verifyFn.accept(BatchBuilder.scanResultValue("dummy", schema, ScanQuery.ResultFormat.RESULT_FORMAT_LIST));
  }

  private static void verifyEmptySchema(BatchBuilder batchBuilder)
  {
    RowSchema schema = batchBuilder.schema();
    Batch batch = batchBuilder.build();
    BatchCursor cursor = batch.newCursor();

    assertSame(schema, cursor.columns().schema());
    ColumnReaderProvider rowReader = cursor.columns();
    assertSame(schema, rowReader.schema());

    RowPositioner positioner = cursor.positioner();
    assertEquals(0, positioner.size());
  }

  @Test
  public void testEmptyBatch()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    verify(schema, ListBatchTest::verifyEmptyBatch);
  }

  private static void verifyEmptyBatch(BatchBuilder batchBuilder)
  {
    RowSchema schema = batchBuilder.schema();
    Batch batch = batchBuilder.build();
    BatchCursor cursor = batch.newCursor();

    // Can obtain column readers but nothing to read
    ColumnReaderProvider rowReader = cursor.columns();
    assertSame(schema, rowReader.schema());
    assertSame(schema.column(0), rowReader.scalar(0).schema());
    assertSame(schema.column("b"), rowReader.scalar("b").schema());

    RowPositioner positioner = cursor.positioner();
    assertEquals(0, positioner.size());
  }

  @Test
  public void testSingleColumn()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .build();
    verify(schema, ListBatchTest::verifySingleColumn);
  }

  private static void verifySingleColumn(BatchBuilder batchBuilder)
  {
    RowSchema schema = batchBuilder.schema();

    // Use singletonRow(), mostly in the case that the one
    // column is an array, but also works for scalars.
    Batch batch = batchBuilder
        .singletonRow("first")
        .singletonRow("second")
        .build();
    BatchCursor cursor = batch.newCursor();
    ColumnReaderProvider rowReader = cursor.columns();

    RowPositioner positioner = cursor.positioner();
    assertEquals(2, positioner.size());
    assertSame(schema, rowReader.schema());

    assertEquals(-1, positioner.index());
    assertTrue(positioner.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertTrue(positioner.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertFalse(positioner.next());

    positioner.reset();
    assertTrue(positioner.seek(1));
    assertEquals("second", rowReader.scalar(0).getString());
    assertTrue(positioner.seek(0));
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
    verify(schema, ListBatchTest::verifyMultipleColumns);
  }

  private static void verifyMultipleColumns(BatchBuilder batchBuilder)
  {
    Batch batch = batchBuilder
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchCursor cursor = batch.newCursor();
    ColumnReaderProvider rowReader = cursor.columns();

    RowPositioner positioner = cursor.positioner();
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
    verify(schema, ListBatchTest::verifyMultipleBatches);
  }

  private static void verifyMultipleBatches(BatchBuilder batchBuilder)
  {
    Batch batch = batchBuilder
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchCursor cursor = batch.newCursor();
    ColumnReaderProvider rowReader = cursor.columns();

    RowPositioner positioner = cursor.positioner();
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
    verify(schema, ListBatchTest::verifAllScalarTypes);
  }

  private static void verifAllScalarTypes(BatchBuilder batchBuilder)
  {
    Batch batch = batchBuilder
        .row(1L, null, null, null, null, null)
        .row(2L, "second", 10f, 20D, new Object(), null)
        .build();
    BatchCursor cursor = batch.newCursor();
    ColumnReaderProvider rowReader = cursor.columns();

    // Reader does actual reading.
    ScalarColumnReader lReader = rowReader.scalar(0);
    ScalarColumnReader sReader = rowReader.scalar("s");
    ScalarColumnReader fReader = rowReader.scalar(2);
    ScalarColumnReader dReader = rowReader.scalar("d");
    ScalarColumnReader oReader = rowReader.scalar(4);
    ScalarColumnReader nReader = rowReader.scalar("n");

    RowPositioner positioner = cursor.positioner();
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
