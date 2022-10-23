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
import org.apache.druid.exec.operator.impl.Batches;
import org.apache.druid.exec.util.BatchBuilder;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Test the schema inference aspect of the scan result batch.
 * Basic functionality is covered in {@link ListBatchTest}.
 */
public class ScanResultValueBatchTest
{
  @Test
  public void testCapabilities()
  {
    ScanResultValueBatch batch = new ScanResultValueBatch(Batches.emptySchema(), ScanQuery.ResultFormat.RESULT_FORMAT_LIST, null);
    BatchCapabilities cap = batch.capabilities();
    assertEquals(BatchFormat.SCAN_MAP, cap.format());
    assertTrue(cap.canSeek());
    assertFalse(cap.canSort());

    batch = new ScanResultValueBatch(Batches.emptySchema(), ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST, null);
    cap = batch.capabilities();
    assertEquals(BatchFormat.SCAN_OBJECT_ARRAY, cap.format());
    assertTrue(cap.canSeek());
    assertFalse(cap.canSort());
  }

  @Test
  public void testEmptySchema()
  {
    ScanResultValue scanValue = new ScanResultValue("foo", Collections.emptyList(), Collections.emptyList());
    Batch batch = ScanResultValueBatch.of(scanValue);

    // Batch is empty
    BatchReader reader = batch.newReader();
    BatchCursor cursor = reader.cursor();
    assertEquals(0, cursor.size());
    assertEquals(0, reader.columns().schema().size());
    assertFalse(cursor.next());
  }

  @Test
  public void testEmptyBatch()
  {
    ScanResultValue scanValue = new ScanResultValue("foo", Arrays.asList("a", "b"), Collections.emptyList());
    Batch batch = ScanResultValueBatch.of(scanValue);
    BatchReader reader = batch.newReader();
    ColumnReaderFactory columns = reader.columns();

    // Batch is empty
    BatchCursor cursor = reader.cursor();
    assertEquals(0, cursor.size());

    // Schema was inferred, but only names.
    RowSchema expected = new SchemaBuilder()
        .scalar("a", null)
        .scalar("b", null)
        .build();
    assertEquals(expected, columns.schema());

    // Reader only does EOF
    assertNotNull(columns.scalar(0));
    assertSame(columns.scalar(0), columns.scalar("a"));
    assertNotNull(columns.scalar(1));
    assertSame(columns.scalar(1), columns.scalar("b"));
  }

  @Test
  public void testCompactList()
  {
    List<Object[]> rows = Arrays.asList(
        new Object[] {1L, null, null, null, null, null},
        new Object[] {2L, "second", 10f, 20D, new Object(), null}
    );
    ScanResultValue scanValue = new ScanResultValue(
        "foo",
        Arrays.asList("l", "s", "f", "d", "o", "n"),
        rows
    );
    Batch batch = ScanResultValueBatch.of(scanValue);
    validateBatch(batch);
  }

  private void validateBatch(Batch batch)
  {
    BatchReader reader = batch.newReader();
    assertEquals(2, reader.cursor().size());

    // Check inferred schema
    RowSchema expected = new SchemaBuilder()
        .scalar("l", ColumnType.LONG)
        .scalar("s", ColumnType.STRING)
        .scalar("f", ColumnType.FLOAT)
        .scalar("d", ColumnType.DOUBLE)
        .scalar("o", ColumnType.UNKNOWN_COMPLEX)
        .scalar("n", null)
        .build();
    assertEquals(expected, reader.columns().schema());
  }

  @Test
  public void testMapList()
  {
    // Using a hash map because it allows nulls.
    Map<String, Object> row1 = new HashMap<>();
    row1.put("l", 1L);
    row1.put("s", null);
    row1.put("f", null);
    row1.put("d", null);
    row1.put("o", null);
    row1.put("n", null);
    Map<String, Object> row2 = new HashMap<>();
    row2.put("l", 2L);
    row2.put("s", "second");
    row2.put("f", 10F);
    row2.put("d", 20D);
    row2.put("o", new Object());
    row2.put("n", null);
    List<Map<String, Object>> rows = Arrays.asList(row1, row2);
    ScanResultValue scanValue = new ScanResultValue(
        "foo",
        Arrays.asList("l", "s", "f", "d", "o", "n"),
        rows
    );
    Batch batch = ScanResultValueBatch.of(scanValue);
    validateBatch(batch);
  }

  @Test
  public void testDirectCopy()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();

    ScanResultValueWriter writer = new ScanResultValueWriter(
        "foo",
        schema,
        ScanQuery.ResultFormat.RESULT_FORMAT_LIST,
        Integer.MAX_VALUE
    );
    writer.newBatch();

    BatchBuilder batchBuilder = BatchBuilder.scanResultValue(
        "bar",
        schema,
        ScanQuery.ResultFormat.RESULT_FORMAT_LIST
    );
    Batch batch = batchBuilder
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader reader = batch.newReader();

    assertTrue(writer.canDirectCopyFrom(reader));
    writer.directCopy(reader, 10);
    assertTrue(reader.cursor().isEOF());

    batchBuilder.newBatch();
    batch = batchBuilder
        .row("third", 3)
        .row("fourth", 4)
        .row("fifth", 5)
        .build();
    BatchReader reader2 = batch.bindReader(reader);
    assertSame(reader2, reader);

    writer.directCopy(reader, 1);
    assertEquals(1, reader.cursor().index());
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

    BatchValidator.assertEquals(expected, writer.harvest());

    // Cannot direct copy across formats.
    ScanResultValueWriter incompat = new ScanResultValueWriter(
        "foo",
        schema,
        ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST,
        Integer.MAX_VALUE
    );
    assertFalse(incompat.canDirectCopyFrom(reader));
  }
}
