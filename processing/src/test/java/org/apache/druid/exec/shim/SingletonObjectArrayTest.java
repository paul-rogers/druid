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

import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

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
    assertSame(SingletonObjectArrayBatchType.INSTANCE, reader.schema().type());
    assertSame(schema, reader.columns().schema());
    ColumnReaderProvider rowReader = reader.columns();
    assertSame(schema, rowReader.schema());
    assertEquals(0, reader.size());

    AtomicInteger lastSize = new AtomicInteger(-2);
    reader.bindListener(size -> lastSize.set(size));
    assertEquals(0, lastSize.get());

    reader.bind(new Object[] {});
    assertEquals(1, lastSize.get());
    assertEquals(1, reader.size());

    reader.bind(null);;
    assertEquals(0, lastSize.get());
    assertEquals(0, reader.size());
  }

  @Test
  public void testSingleColumn()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .build();

    SingletonObjectArrayReader reader = new SingletonObjectArrayReader(schema);

    AtomicInteger lastSize = new AtomicInteger(-2);
    reader.bindListener(size -> lastSize.set(size));
    reader.bind(new Object[] { "first" } );
    assertEquals(1, lastSize.get());
    assertEquals(1, reader.size());

    assertSame(schema, reader.columns().schema());
    ColumnReaderProvider rowReader = reader.columns();
    assertSame(schema, rowReader.schema());

    reader.updatePosition(-1);
    assertTrue(reader.columns().scalar(0).isNull());

    reader.updatePosition(0);
    assertEquals("first", rowReader.scalar(0).getString());

    reader.bind(null);
    assertTrue(reader.columns().scalar(0).isNull());
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
    SingletonObjectArrayReader cursor = new SingletonObjectArrayReader(schema);
    ColumnReaderProvider columns = cursor.columns();

    // Pass an int value for "b". Should map to the defined
    // type.
    cursor.bind(new Object[] { "first", 1 } );

    assertEquals("first", columns.scalar(0).getString());
    assertEquals(1L, columns.scalar(1).getLong());

    // Second batch
    cursor.bind(new Object[] { "second", 2L } );

    assertEquals("second", columns.scalar(0).getString());
    assertEquals(2L, columns.scalar(1).getLong());
  }
}
