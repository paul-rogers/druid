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

package org.apache.druid.exec.test;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.ResultIterator.EofException;
import org.apache.druid.exec.operator.ResultIterator.StallException;
import org.apache.druid.exec.shim.ObjectArrayListBatchType;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class SimpleDataGenTest
{
  @Test
  public void testNoColumns()
  {
    SimpleDataGenSpec spec = new SimpleDataGenSpec(
        1,
        Collections.emptyList(),
        BatchFormat.OBJECT_ARRAY,
        5,
        0
    );
    FragmentContext context = TestUtils.emptyFragment();
    SimpleDataGenOperator op = new SimpleDataGenOperator(context, spec);
    assertEquals(spec.format, op.batchSchema().type().format());
    assertEquals(0, op.batchSchema().rowSchema().size());

    ResultIterator<?> iter = op.open();
    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testNoRows()
  {
    SimpleDataGenSpec spec = new SimpleDataGenSpec(
        1,
        Arrays.asList("str", "rid", "bob"),
        BatchFormat.OBJECT_ARRAY,
        5,
        0
    );
    FragmentContext context = TestUtils.emptyFragment();
    SimpleDataGenOperator op = new SimpleDataGenOperator(context, spec);
    assertEquals(spec.format, op.batchSchema().type().format());
    assertEquals(3, op.batchSchema().rowSchema().size());

    ResultIterator<?> iter = op.open();
    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testOneBatch() throws StallException
  {
    SimpleDataGenSpec spec = new SimpleDataGenSpec(
        1,
        Arrays.asList("str", "rid", "str5", "rand", "bob"),
        BatchFormat.OBJECT_ARRAY,
        10,
        8
    );
    FragmentContext context = TestUtils.emptyFragment();
    SimpleDataGenOperator op = new SimpleDataGenOperator(context, spec);

    ResultIterator<?> iter = op.open();

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("str", ColumnType.STRING)
        .scalar("rid", ColumnType.LONG)
        .scalar("str5", ColumnType.STRING)
        .scalar("rand", ColumnType.LONG)
        .scalar("bob", ColumnType.STRING)
        .build();
    assertEquals(expectedSchema, op.batchSchema().rowSchema());

    Batch actual = op.batchSchema().of(iter.next());
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row("Row 1", 1, "Rot 1", 1, null)
        .row("Row 2", 2, "Rot 2", 2, null)
        .row("Row 3", 3, "Rot 3", 3, null)
        .row("Row 4", 4, "Rot 4", 4, null)
        .row("Row 5", 5, "Rot 0", 5, null)
        .row("Row 6", 6, "Rot 1", 6, null)
        .row("Row 7", 7, "Rot 2", 0, null)
        .row("Row 8", 8, "Rot 3", 1, null)
        .build();
    BatchValidator.assertEquals(expected, actual);

    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testMultipleBatches() throws StallException
  {
    SimpleDataGenSpec spec = new SimpleDataGenSpec(
        1,
        Arrays.asList("rid", "str"),
        BatchFormat.OBJECT_ARRAY,
        3,
        8
    );
    FragmentContext context = TestUtils.emptyFragment();
    SimpleDataGenOperator op = new SimpleDataGenOperator(context, spec);

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("str", ColumnType.STRING)
        .build();
    Batch actual = ObjectArrayListBatchType.INSTANCE.newBatch(expectedSchema);

    ResultIterator<?> iter = op.open();

    actual.bind(op.next());
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, "Row 1")
        .row(2, "Row 2")
        .row(3, "Row 3")
        .build();
    BatchValidator.assertEquals(expected, actual);

    actual.bind(op.next());
    expected = BatchBuilder.arrayList(expectedSchema)
        .row(4, "Row 4")
        .row(5, "Row 5")
        .row(6, "Row 6")
        .build();
    BatchValidator.assertEquals(expected, actual);

    actual.bind(op.next());
    expected = BatchBuilder.arrayList(expectedSchema)
        .row(7, "Row 7")
        .row(8, "Row 8")
        .build();
    BatchValidator.assertEquals(expected, actual);
    assertThrows(EofException.class, () -> iter.next());
  }
}
