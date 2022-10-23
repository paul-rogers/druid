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

package org.apache.druid.exec.util;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;

public class BatchValidatorTest
{
  @Test
  public void testEmptySchema()
  {
    RowSchema schema = new SchemaBuilder().build();
    Batch batch1 = BatchBuilder.arrayList(schema).build();
    Batch batch2 = BatchBuilder.arrayList(schema).build();
    BatchReader reader1 = batch1.newReader();
    BatchReader reader2 = batch2.newReader();

    // Test the null/not null variations. A check in one case is sufficient.
    BatchValidator.assertEquals((Batch) null, null);
    BatchValidator.assertNotEquals(batch1, null);
    BatchValidator.assertNotEquals(null, batch2);
    BatchValidator.assertEquals(batch1, batch2);

    // Test the null/not null variations. A check in one case is sufficient.
    BatchValidator.assertEquals((BatchReader) null, null);
    BatchValidator.assertNotEquals(reader1, null);
    BatchValidator.assertNotEquals(null, reader2);
    BatchValidator.assertEquals(reader1, reader2);
  }

  @Test
  public void testEmptyBatch()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch batch1 = BatchBuilder.arrayList(schema).build();
    Batch batch2 = BatchBuilder.arrayList(schema).build();

    BatchValidator.assertEquals(batch1, batch2);

    // Test the row reader variation one more time. This test is sufficient
    // as the later batch reader tests call the row reader validation.
    BatchValidator.assertEquals(batch1.newReader(), batch2.newReader());

    // Vs. an empty schema
    RowSchema schema2 = new SchemaBuilder().build();
    Batch batch3 = BatchBuilder.arrayList(schema2).build();
    BatchValidator.assertNotEquals(batch1, batch3);
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
    Batch batch1 = BatchBuilder.arrayList(schema)
        .row("first")
        .row("second")
        .build();
    Batch batch2 = BatchBuilder.arrayList(schema)
        .row("first")
        .row("second")
        .build();

    BatchValidator.assertEquals(batch1, batch2);

    // Vs. an empty batch
    Batch batch3 = BatchBuilder.arrayList(schema).build();
    BatchValidator.assertNotEquals(batch1, batch3);
    BatchValidator.assertNotEquals(batch3, batch1);

    // Vs. a different schema
    RowSchema schema2 = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch batch4 = BatchBuilder.arrayList(schema2)
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchValidator.assertNotEquals(batch1, batch4);
    BatchValidator.assertNotEquals(batch4, batch1);

    // Vs. short results
    Batch batch5 = BatchBuilder.arrayList(schema)
        .row("first")
        .build();
    BatchValidator.assertNotEquals(batch1, batch5);
    BatchValidator.assertNotEquals(batch5, batch1);

    // Vs. different results
    Batch batch6 = BatchBuilder.arrayList(schema)
        .row("first")
        .row("not second")
        .build();
    BatchValidator.assertNotEquals(batch1, batch6);
    BatchValidator.assertNotEquals(batch6, batch1);

    // Vs. null values
    Batch batch7 = BatchBuilder.arrayList(schema)
        .row("first")
        .singletonRow(null)
        .build();
    BatchValidator.assertNotEquals(batch1, batch7);
    BatchValidator.assertNotEquals(batch7, batch1);
  }

  @Test
  public void testLong()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.LONG)
        .build();
    Batch batch1 = BatchBuilder.arrayList(schema)
        .row(1) // Normal mapping of int to long
        .row(2)
        .build();
    Batch batch2 = BatchBuilder.arrayList(schema)
        .row(1L) // Just to double-check
        .row(2L)
        .build();

    BatchValidator.assertEquals(batch1, batch2);

    // Differing results
    Batch batch3 = BatchBuilder.arrayList(schema)
        .row(1)
        .row(3)
        .build();
    BatchValidator.assertNotEquals(batch1, batch3);
  }

  @Test
  public void testFloat()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.FLOAT)
        .build();
    Batch batch1 = BatchBuilder.arrayList(schema)
        .row(1.1)
        .row(2.2)
        .build();
    Batch batch2 = BatchBuilder.arrayList(schema)
        .row(1.10001) // Within tolerance
        .row(2.20002)
        .build();

    BatchValidator.assertEquals(batch1, batch2);

    // Differing results
    Batch batch3 = BatchBuilder.arrayList(schema)
        .row(1.1)
        .row(2.3)
        .build();
    BatchValidator.assertNotEquals(batch1, batch3);
  }

  @Test
  public void testDouble()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.DOUBLE)
        .build();
    Batch batch1 = BatchBuilder.arrayList(schema)
        .row(1.1f) // To test auto-conversion
        .row(2.2f)
        .build();
    Batch batch2 = BatchBuilder.arrayList(schema)
        .row(1.1000001D) // Within tolerance
        .row(2.2000002D)
        .build();

    BatchValidator.assertEquals(batch1, batch2);

    // Differing results
    Batch batch3 = BatchBuilder.arrayList(schema)
        .row(1.1D)
        .row(2.3D)
        .build();
    BatchValidator.assertNotEquals(batch1, batch3);
  }

  @Test
  public void testObject()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.UNKNOWN_COMPLEX)
        .build();
    Batch batch1 = BatchBuilder.arrayList(schema)
        .row(Arrays.asList("p", "q"))
        .row(Arrays.asList("r", "s"))
        .build();
    Batch batch2 = BatchBuilder.arrayList(schema)
        .row(Arrays.asList("p", "q"))
        .row(Arrays.asList("r", "s"))
        .build();

    BatchValidator.assertEquals(batch1, batch2);

    // Differing results
    Batch batch3 = BatchBuilder.arrayList(schema)
        .row(Arrays.asList("p", "q"))
        .row(Arrays.asList("r", "t"))
        .build();
    BatchValidator.assertNotEquals(batch1, batch3);
  }

  @Test
  public void testDifferentImplementations()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();
    Batch batch1 = BatchBuilder.arrayList(schema)
        .row("first", 1)
        .row("second", 2)
        .build();
    Batch batch2 = BatchBuilder.mapList(schema)
        .row("first", 1)
        .row("second", 2)
        .build();

    BatchValidator.assertEquals(batch1, batch2);
  }
}
