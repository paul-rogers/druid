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

import org.apache.druid.queryng.rows.Batch.BatchReader;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class BatchValidatorTest
{
  @Test
  public void testEmptySchema()
  {
    RowSchema schema = new SchemaBuilder().build();
    List<Object[]> batch1 = BatchBuilder.arrayList(schema).build();
    List<Object[]> batch2 = BatchBuilder.arrayList(schema).build();
    BatchReader<List<Object[]>> reader1 = ObjectArrayListReader.of(schema, batch1);
    BatchReader<List<Object[]>> reader2 = ObjectArrayListReader.of(schema, batch2);

    // Test the null/not null variations. A check in one case is sufficient.
    BatchValidator.assertEquals((BatchReader<?>) null, null);
    BatchValidator.assertNotEquals(reader1, null);
    BatchValidator.assertNotEquals(null, reader2);
    BatchValidator.assertEquals(reader1, reader2);

    RowReader rowReader1 = reader1.reader();
    RowReader rowReader2 = reader2.reader();
    BatchValidator.assertEquals((RowReader) null, null);
    BatchValidator.assertNotEquals(rowReader1, null);
    BatchValidator.assertNotEquals(null, rowReader2);
    BatchValidator.assertEquals(rowReader1, rowReader2);
  }

  @Test
  public void testEmptyBatch()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    List<Object[]> batch1 = BatchBuilder.arrayList(schema).build();
    List<Object[]> batch2 = BatchBuilder.arrayList(schema).build();
    BatchReader<List<Object[]>> reader1 = ObjectArrayListReader.of(schema, batch1);
    BatchReader<List<Object[]>> reader2 = ObjectArrayListReader.of(schema, batch2);

    BatchValidator.assertEquals(reader1, reader2);

    // Test the row reader variation one more time. This test is sufficient
    // as the later batch reader tests call the row reader validation.
    RowReader rowReader1 = reader1.reader();
    RowReader rowReader2 = reader2.reader();
    BatchValidator.assertEquals(rowReader1, rowReader2);

    // Vs. an empty schema
    RowSchema schema2 = new SchemaBuilder().build();
    BatchReader<List<Object[]>> reader3 = ObjectArrayListReader.of(schema2, batch1);
    BatchValidator.assertNotEquals(reader1, reader3);
    RowReader rowReader3 = reader3.reader();
    BatchValidator.assertNotEquals(rowReader1, rowReader3);
  }

  /**
   * Test strings. Also tests the variations of short-long rows, mismatched
   * schema. These variations need be tested only once, in this one case.
   */
  @Test
  public void testString()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .build();
    List<Object[]> batch1 = BatchBuilder.arrayList(schema)
        .row("first")
        .row("second")
        .build();
    List<Object[]> batch2 = BatchBuilder.arrayList(schema)
        .row("first")
        .row("second")
        .build();
    BatchReader<List<Object[]>> reader1 = ObjectArrayListReader.of(schema, batch1);
    BatchReader<List<Object[]>> reader2 = ObjectArrayListReader.of(schema, batch2);

    BatchValidator.assertEquals(reader1, reader2);

    RowReader rowReader1 = reader1.reader();
    RowReader rowReader2 = reader2.reader();
    BatchValidator.assertEquals(rowReader1, rowReader2);

    // Vs. an empty batch
    List<Object[]> batch3 = BatchBuilder.arrayList(schema).build();
    BatchReader<List<Object[]>> reader3 = ObjectArrayListReader.of(schema, batch3);
    BatchValidator.assertNotEquals(reader1, reader3);
    BatchValidator.assertNotEquals(rowReader1, reader3.reader());

    // Vs. a different schema
    RowSchema schema2 = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    List<Object[]> batch4 = BatchBuilder.arrayList(schema2)
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader<List<Object[]>> reader4 = ObjectArrayListReader.of(schema2, batch4);
    BatchValidator.assertNotEquals(reader1, reader4);
    BatchValidator.assertNotEquals(rowReader1, reader4.reader());

    // Vs. short results
    List<Object[]> batch5 = BatchBuilder.arrayList(schema)
        .row("first")
        .build();
    BatchReader<List<Object[]>> reader5 = ObjectArrayListReader.of(schema, batch5);
    BatchValidator.assertNotEquals(reader1, reader5);
    BatchValidator.assertNotEquals(reader5, reader1);

    // Vs. different results
    List<Object[]> batch6 = BatchBuilder.arrayList(schema)
        .row("first")
        .row("not second")
        .build();
    BatchReader<List<Object[]>> reader6 = ObjectArrayListReader.of(schema, batch6);
    BatchValidator.assertNotEquals(reader1, reader6);
    BatchValidator.assertNotEquals(reader6, reader1);

    // Vs. null values
    List<Object[]> batch7 = BatchBuilder.arrayList(schema)
        .row("first")
        .singletonRow(null)
        .build();
    BatchReader<List<Object[]>> reader7 = ObjectArrayListReader.of(schema, batch7);
    BatchValidator.assertNotEquals(reader1, reader7);
    BatchValidator.assertNotEquals(reader7, reader1);
  }

  @Test
  public void testLong()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.LONG)
        .build();
    List<Object[]> batch1 = BatchBuilder.arrayList(schema)
        .row(1) // Normal mapping of int to long
        .row(2)
        .build();
    List<Object[]> batch2 = BatchBuilder.arrayList(schema)
        .row(1L) // Just to double-check
        .row(2L)
        .build();
    BatchReader<List<Object[]>> reader1 = ObjectArrayListReader.of(schema, batch1);
    BatchReader<List<Object[]>> reader2 = ObjectArrayListReader.of(schema, batch2);

    BatchValidator.assertEquals(reader1, reader2);

    // Differing results
    List<Object[]> batch3 = BatchBuilder.arrayList(schema)
        .row(1)
        .row(3)
        .build();
    BatchReader<List<Object[]>> reader3 = ObjectArrayListReader.of(schema, batch3);
    BatchValidator.assertNotEquals(reader1, reader3);
  }

  @Test
  public void testFloat()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.FLOAT)
        .build();
    List<Object[]> batch1 = BatchBuilder.arrayList(schema)
        .row(1.1)
        .row(2.2)
        .build();
    List<Object[]> batch2 = BatchBuilder.arrayList(schema)
        .row(1.10001) // Within tolerance
        .row(2.20002)
        .build();
    BatchReader<List<Object[]>> reader1 = ObjectArrayListReader.of(schema, batch1);
    BatchReader<List<Object[]>> reader2 = ObjectArrayListReader.of(schema, batch2);

    BatchValidator.assertEquals(reader1, reader2);

    // Differing results
    List<Object[]> batch3 = BatchBuilder.arrayList(schema)
        .row(1.1)
        .row(2.3)
        .build();
    BatchReader<List<Object[]>> reader3 = ObjectArrayListReader.of(schema, batch3);
    BatchValidator.assertNotEquals(reader1, reader3);
  }

  @Test
  public void testDouble()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.DOUBLE)
        .build();
    List<Object[]> batch1 = BatchBuilder.arrayList(schema)
        .row(1.1f) // To test auto-conversion
        .row(2.2f)
        .build();
    List<Object[]> batch2 = BatchBuilder.arrayList(schema)
        .row(1.1000001D) // Within tolerance
        .row(2.2000002D)
        .build();
    BatchReader<List<Object[]>> reader1 = ObjectArrayListReader.of(schema, batch1);
    BatchReader<List<Object[]>> reader2 = ObjectArrayListReader.of(schema, batch2);

    BatchValidator.assertEquals(reader1, reader2);

    // Differing results
    List<Object[]> batch3 = BatchBuilder.arrayList(schema)
        .row(1.1D)
        .row(2.3D)
        .build();
    BatchReader<List<Object[]>> reader3 = ObjectArrayListReader.of(schema, batch3);
    BatchValidator.assertNotEquals(reader1, reader3);
  }

  @Test
  public void testObject()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.UNKNOWN_COMPLEX)
        .build();
    List<Object[]> batch1 = BatchBuilder.arrayList(schema)
        .row(Arrays.asList("p", "q"))
        .row(Arrays.asList("r", "s"))
        .build();
    List<Object[]> batch2 = BatchBuilder.arrayList(schema)
        .row(Arrays.asList("p", "q"))
        .row(Arrays.asList("r", "s"))
        .build();
    BatchReader<List<Object[]>> reader1 = ObjectArrayListReader.of(schema, batch1);
    BatchReader<List<Object[]>> reader2 = ObjectArrayListReader.of(schema, batch2);

    BatchValidator.assertEquals(reader1, reader2);

    // Differing results
    List<Object[]> batch3 = BatchBuilder.arrayList(schema)
        .row(Arrays.asList("p", "q"))
        .row(Arrays.asList("r", "t"))
        .build();
    BatchReader<List<Object[]>> reader3 = ObjectArrayListReader.of(schema, batch3);
    BatchValidator.assertNotEquals(reader1, reader3);
  }

  @Test
  public void testDifferentImplementations()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    List<Object[]> batch1 = BatchBuilder.arrayList(schema)
        .row("first", 1)
        .row("second", 2)
        .build();
    List<Map<String, Object>> batch2 = BatchBuilder.mapList(schema)
        .row("first", 1)
        .row("second", 2)
        .build();

    BatchReader<List<Object[]>> reader1 = ObjectArrayListReader.of(schema, batch1);
    BatchReader<List<Map<String, Object>>> reader2 = MapListReader.of(schema, batch2);

    BatchValidator.assertEquals(reader1, reader2);
  }
}
