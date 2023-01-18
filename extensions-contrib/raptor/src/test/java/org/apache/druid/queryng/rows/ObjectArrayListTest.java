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

package org.apache.druid.queryng.rows;

import org.apache.druid.queryng.rows.RowReader.ScalarColumnReader;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test the object array batch, which tests many of the component
 * pieces. Note: we do not use the batch validator here because
 * the batch validator requires this code to work.
 */
public class ObjectArrayListTest
{
  @Test
  public void testEmptySchema()
  {
    RowSchema schema = new SchemaBuilder().build();
    List<Object[]> batch = BatchBuilder.arrayList(schema).build();
    BatchReader<List<Object[]>> reader = new ObjectArrayListReader(schema);
    reader.bind(batch);

    assertSame(schema, reader.schema());
    RowReader rowReader = reader.row();
    assertSame(schema, rowReader.schema());

    assertEquals(0, reader.size());
    assertEquals(-1, reader.index());
    assertFalse(reader.next());

    reader.reset();
    assertEquals(-1, reader.index());

    assertFalse(reader.seek(-1));
    assertFalse(reader.seek(1));
    assertEquals(0, reader.index());
  }

  @Test
  public void testEmptyBatch()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    List<Object[]> batch = BatchBuilder.arrayList(schema).build();
    BatchReader<List<Object[]>> reader = ObjectArrayListReader.of(schema, batch);

    assertSame(schema, reader.schema());
    assertEquals(2, reader.size());
    RowReader rowReader = reader.row();
    assertSame(schema, rowReader.schema());

    assertEquals(-1, reader.index());
    assertTrue(reader.next());
    assertEquals(0, reader.index());
    assertEquals("first", rowReader.scalar(0).getString());
    assertTrue(reader.next());
    assertEquals(1, reader.index());
    assertEquals("second", rowReader.scalar(0).getString());
    assertFalse(reader.next());
    assertEquals(2, reader.index());

    // OK to move next at EOF, stays at EOF.
    assertFalse(reader.next());
    assertEquals(2, reader.index());

    reader.reset();
    assertEquals(-1, reader.index());

    assertTrue(reader.seek(1));
    assertEquals(1, reader.index());
    assertEquals("second", rowReader.scalar(0).getString());
    assertTrue(reader.seek(0));
    assertEquals(0, reader.index());
    assertEquals("first", rowReader.scalar(0).getString());
  }

  @Test
  public void testSingleColumn()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .build();

    // Use singletonRow(), mostly in the case that the one
    // column is an array, but also works for scalars.
    List<Object[]> batch = BatchBuilder.arrayList(schema)
        .singletonRow("first")
        .singletonRow("second")
        .build();
    BatchReader<List<Object[]>> reader = ObjectArrayListReader.of(schema, batch);

    assertSame(schema, reader.schema());
    assertEquals(2, reader.size());
    RowReader rowReader = reader.row();
    assertSame(schema, rowReader.schema());

    assertTrue(reader.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertTrue(reader.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertFalse(reader.next());
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
    List<Object[]> batch = BatchBuilder.arrayList(schema)
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader<List<Object[]>> reader = ObjectArrayListReader.of(schema, batch);
    RowReader rowReader = reader.row();

    assertTrue(reader.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertEquals(1L, rowReader.scalar(1).getLong());
    assertTrue(reader.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertEquals(2L, rowReader.scalar(1).getLong());
    assertFalse(reader.next());
  }

  @Test
  public void testMultipleBatches()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();

    // First batch & setup
    BatchBuilder<List<Object[]>> batchBuilder = BatchBuilder.arrayList(schema);
    List<Object[]> batch = batchBuilder
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader<List<Object[]>> reader = new ObjectArrayListReader(schema);
    reader.bind(batch);
    RowReader rowReader = reader.row();

    assertTrue(reader.next());
    assertEquals("first", rowReader.scalar(0).getString());
    assertEquals(1L, rowReader.scalar(1).getLong());
    assertTrue(reader.next());
    assertEquals("second", rowReader.scalar(0).getString());
    assertEquals(2L, rowReader.scalar(1).getLong());
    assertFalse(reader.next());

    // Second batch
    batch = batchBuilder
        .row("third", 3)
        .row("fourth", 4)
        .build();
    reader.bind(batch);

    assertTrue(reader.next());
    assertEquals("third", rowReader.scalar(0).getString());
    assertEquals(3L, rowReader.scalar(1).getLong());
    assertTrue(reader.next());
    assertEquals("fourth", rowReader.scalar(0).getString());
    assertEquals(4L, rowReader.scalar(1).getLong());
    assertFalse(reader.next());
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
    List<Object[]> batch = BatchBuilder.arrayList(schema)
        .row(1L, null, null, null, null, null)
        .row(2L, "second", 10f, 20D, new Object(), null)
        .build();
    BatchReader<List<Object[]>> reader = ObjectArrayListReader.of(schema, batch);

    // Reader does actual reading.
    RowReader rowReader = reader.row();
    ScalarColumnReader lReader = rowReader.scalar(0);
    ScalarColumnReader sReader = rowReader.scalar("s");
    ScalarColumnReader fReader = rowReader.scalar(2);
    ScalarColumnReader dReader = rowReader.scalar("d");
    ScalarColumnReader oReader = rowReader.scalar(4);
    ScalarColumnReader nReader = rowReader.scalar("n");

    assertTrue(reader.next());
    assertFalse(lReader.isNull());
    assertEquals(1L, lReader.getLong());
    assertTrue(sReader.isNull());
    assertTrue(nReader.isNull());

    assertTrue(reader.next());
    assertFalse(lReader.isNull());
    assertEquals(2L, lReader.getLong());
    assertFalse(sReader.isNull());
    assertEquals("second", sReader.getString());
    assertEquals(10D, fReader.getDouble(), 0.000001);
    assertEquals(20D, dReader.getDouble(), 0.000001);
    assertTrue(oReader.getObject() instanceof Object);
    assertTrue(nReader.isNull());

    assertFalse(reader.next());
  }
}
