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

package org.apache.druid.exec.internalSort;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.fragment.FragmentManager;
import org.apache.druid.exec.fragment.Fragments;
import org.apache.druid.exec.fragment.OperatorConverter;
import org.apache.druid.exec.operator.Operators;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.ResultIterator.EofException;
import org.apache.druid.exec.operator.ResultIterator.StallException;
import org.apache.druid.exec.plan.FragmentSpec;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.exec.plan.InternalSortOp.SortType;
import org.apache.druid.exec.plan.OperatorSpec;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.test.SimpleDataGenFactory;
import org.apache.druid.exec.test.SimpleDataGenSpec;
import org.apache.druid.exec.test.TestUtils;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RowInternalSortTest
{
  private final OperatorConverter CONVERTER = new OperatorConverter(
      Collections.singletonList(new SimpleDataGenFactory())
  );

  public FragmentManager build(OperatorSpec...ops)
  {
    return TestUtils.fragment(CONVERTER, Arrays.asList(ops));
  }

  @Test
  public void testEmptyInput()
  {
    InternalSortOp sortSpec = new InternalSortOp(
        1,
        2,
        SortType.ROW,
        Collections.singletonList(new SortColumn("rid", false))
    );
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        2,
        Collections.singletonList("rid"),
        BatchFormat.OBJECT_ARRAY,
        5,
        0
    );
    FragmentManager fragment = build(readerSpec, sortSpec);
    ResultIterator<?> iter = fragment.run();
    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testSingleBatchSingleKey() throws StallException
  {
    InternalSortOp sortSpec = new InternalSortOp(
        1,
        2,
        SortType.ROW,
        Collections.singletonList(new SortColumn("rand", false))
    );
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        2,
        Arrays.asList("rid", "rand"),
        BatchFormat.OBJECT_ARRAY,
        10,
        8
    );
    FragmentManager fragment = build(readerSpec, sortSpec);
    ResultIterator<?> iter = fragment.run();
    BatchSchema batchSchema = Operators.asBatch(fragment.rootOperator()).batchSchema();

    Batch actual = batchSchema.of(iter.next());
    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rand", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, 1)
        .row(6, 1)
        .row(2, 2)
        .row(7, 2)
        .row(3, 3)
        .row(8, 3)
        .row(4, 4)
        .row(5, 5)
        .build();
    BatchValidator.assertEquals(expected, actual);

    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testSingleBatchSingleKeyDesc() throws StallException
  {
    InternalSortOp sortSpec = new InternalSortOp(
        1,
        2,
        SortType.ROW,
        Collections.singletonList(new SortColumn("rand", true))
    );
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        2,
        Arrays.asList("rid", "rand"),
        BatchFormat.OBJECT_ARRAY,
        10,
        8
    );
    FragmentManager fragment = build(readerSpec, sortSpec);
    ResultIterator<?> iter = fragment.run();
    BatchSchema batchSchema = Operators.asBatch(fragment.rootOperator()).batchSchema();

    Batch actual = batchSchema.of(iter.next());
    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rand", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, 1)
        .row(6, 1)
        .row(2, 2)
        .row(7, 2)
        .row(3, 3)
        .row(8, 3)
        .row(4, 4)
        .row(5, 5)
        .build();
    BatchValidator.assertEquals(Batches.reverseOf(expected), actual);

    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testMultipleBatchesSingleKey() throws StallException
  {
    InternalSortOp sortSpec = new InternalSortOp(
        1,
        2,
        SortType.ROW,
        Collections.singletonList(new SortColumn("rand", false))
    );
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        2,
        Arrays.asList("rid", "rand"),
        BatchFormat.OBJECT_ARRAY,
        3,
        8
    );
    FragmentManager fragment = build(readerSpec, sortSpec);
    ResultIterator<?> iter = fragment.run();
    BatchSchema batchSchema = Operators.asBatch(fragment.rootOperator()).batchSchema();

    Batch actual = batchSchema.of(iter.next());
    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rand", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, 1)
        .row(6, 1)
        .row(2, 2)
        .row(7, 2)
        .row(3, 3)
        .row(8, 3)
        .row(4, 4)
        .row(5, 5)
        .build();
    BatchValidator.assertEquals(expected, actual);

    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testMultipleKeysAndBatches() throws StallException
  {
    InternalSortOp sortSpec = new InternalSortOp(
        1,
        2,
        SortType.ROW,
        Arrays.asList(
            new SortColumn("rand", false),
            new SortColumn("str5", true)
        )
    );
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        2,
        Arrays.asList("rand", "str5"),
        BatchFormat.OBJECT_ARRAY,
        10,
        100
    );
    FragmentManager fragment = build(readerSpec, sortSpec);
    ResultIterator<?> iter = fragment.run();
    BatchSchema batchSchema = Operators.asBatch(fragment.rootOperator()).batchSchema();

    Batch actual = batchSchema.of(iter.next());
    assertEquals(100, actual.size());
    BatchCursor cursor = actual.newCursor();
    ScalarColumnReader randReader = cursor.columns().scalar("rand");
    ScalarColumnReader str5Reader = cursor.columns().scalar("str5");
    long lastRand = -1;
    String lastStr5 = "z";
    while (cursor.sequencer().next()) {
      long rand = randReader.getLong();
      assertTrue(rand >= lastRand);
      String str5 = str5Reader.getString();
      if (rand == lastRand) {
        assertTrue(str5.compareTo(lastStr5) <= 0);
      }
      lastRand = rand;
      lastStr5 = str5;
    }
  }

  @Test
  public void adHoc() throws StallException, JsonProcessingException
  {
    for (int i = 0; i < 5; i++) {
      testLargeResultSet();
    }
  }

  @Test
  public void testLargeResultSet() throws StallException, JsonProcessingException
  {
    InternalSortOp sortSpec = new InternalSortOp(
        1,
        2,
        SortType.ROW,
        Arrays.asList(
            new SortColumn("rand", false),
            new SortColumn("str5", true)
        )
    );
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        2,
        Arrays.asList("rand", "str5"),
        BatchFormat.OBJECT_ARRAY,
        100,
        100_000
    );
    FragmentSpec fragSpec = TestUtils.simpleSpec(Arrays.asList(readerSpec, sortSpec));
    FragmentManager fragment = build(readerSpec, sortSpec);
    ResultIterator<?> iter = fragment.run();
    BatchSchema batchSchema = Operators.asBatch(fragment.rootOperator()).batchSchema();

    Batch actual = batchSchema.of(iter.next());
    assertEquals(100_000, actual.size());
    BatchCursor reader = actual.newCursor();
    ScalarColumnReader randReader = reader.columns().scalar("rand");
    ScalarColumnReader str5Reader = reader.columns().scalar("str5");
    long lastRand = -1;
    String lastStr5 = "z";
    while (reader.sequencer().next()) {
      long rand = randReader.getLong();
      assertTrue(rand >= lastRand);
      String str5 = str5Reader.getString();
      if (rand == lastRand) {
        assertTrue(str5.compareTo(lastStr5) <= 0);
      }
      lastRand = rand;
      lastStr5 = str5;
    }

    fragment.close();
    Fragments.logProfile(fragment);
    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    System.out.println(mapper.writeValueAsString(fragSpec));
  }
}
