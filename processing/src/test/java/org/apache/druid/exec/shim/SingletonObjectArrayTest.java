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

import org.apache.druid.exec.batch.BatchReader.BatchCursor;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.ColumnReaderFactory;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class SingletonObjectArrayTest
{
  @Test
  public void testBatchType()
  {
    BatchType batchType = SingletonObjectArrayBatchType.INSTANCE;
    assertEquals(BatchFormat.OBJECT_ARRAY, batchType.format());
    assertFalse(batchType.canSeek());
    assertFalse(batchType.canSort());
    assertFalse(batchType.canWrite());
    assertEquals(0, batchType.sizeOf(null));
    assertEquals(1, batchType.sizeOf(new Object[] {}));
  }

  @Test
  public void testEmptySchema()
  {
    RowSchema schema = new SchemaBuilder().build();
    SingletonObjectArrayReader reader = new SingletonObjectArrayReader(schema);
    assertSame(SingletonObjectArrayBatchType.INSTANCE, reader.factory().type());
    assertSame(schema, reader.columns().schema());
    ColumnReaderFactory rowReader = reader.columns();
    assertSame(schema, rowReader.schema());

    assertTrue(reader.cursor().isEOF());
    assertEquals(0, reader.batchCursor().size());

    reader.bind(new Object[] {});
    assertFalse(reader.cursor().isEOF());
    assertEquals(1, reader.batchCursor().size());
    assertTrue(reader.cursor().next());
    assertFalse(reader.cursor().next());
    assertTrue(reader.cursor().isEOF());
  }

  @Test
  public void testEmptyBatch()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    SingletonObjectArrayReader reader = new SingletonObjectArrayReader(schema);

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

    SingletonObjectArrayReader reader = new SingletonObjectArrayReader(schema);
    reader.bind(new Object[] { "first" } );

    assertSame(schema, reader.columns().schema());
    ColumnReaderFactory rowReader = reader.columns();
    assertSame(schema, rowReader.schema());

    BatchCursor cursor = reader.batchCursor();
    assertEquals(1, cursor.size());
    assertTrue(cursor.next());
    assertEquals("first", rowReader.scalar(0).getString());
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
    SingletonObjectArrayReader reader = new SingletonObjectArrayReader(schema);
    ColumnReaderFactory columns = reader.columns();
    BatchCursor cursor = reader.batchCursor();

    // Pass an int value for "b". Should map to the defined
    // type.
    reader.bind(new Object[] { "first", 1 } );

    assertTrue(cursor.next());
    assertEquals("first", columns.scalar(0).getString());
    assertEquals(1L, columns.scalar(1).getLong());
    assertFalse(cursor.next());

    // Second batch
    reader.bind(new Object[] { "second", 2L } );

    assertTrue(cursor.next());
    assertEquals("second", columns.scalar(0).getString());
    assertEquals(2L, columns.scalar(1).getLong());
    assertFalse(cursor.next());
  }
}
