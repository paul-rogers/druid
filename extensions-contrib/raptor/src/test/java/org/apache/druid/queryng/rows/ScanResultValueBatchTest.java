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

/**
 * Test the schema inference aspect of the scan result batch.
 * Basic functionality is covered in {@link ListBatchTest}.
 */
public class ScanResultValueBatchTest
{
  @Test
  public void testEmptySchema()
  {
    ScanResultValue scanValue = new ScanResultValue("foo", Collections.emptyList(), Collections.emptyList());
    ScanResultValueReader batch = ScanResultValueReader.of(scanValue);

    // Batch is empty
    assertEquals(0, batch.size());
    assertEquals(0, batch.schema().size());

    RowReader reader = batch.row();
    assertSame(batch.schema(), reader.schema());
    assertFalse(batch.next());
  }

  @Test
  public void testEmptyBatch()
  {
    ScanResultValue scanValue = new ScanResultValue("foo", Arrays.asList("a", "b"), Collections.emptyList());
    ScanResultValueReader batch = ScanResultValueReader.of(scanValue);

    // Batch is empty
    assertEquals(0, batch.size());

    // Schema was inferred, but only names.
    RowSchema expected = new SchemaBuilder()
        .scalar("a", null)
        .scalar("b", null)
        .build();
    assertEquals(expected, batch.schema());

    // Reader only does EOF
    RowReader reader = batch.row();
    assertSame(batch.schema(), reader.schema());
    assertNotNull(reader.scalar(0));
    assertSame(reader.scalar(0), reader.scalar("a"));
    assertNotNull(reader.scalar(1));
    assertSame(reader.scalar(1), reader.scalar("b"));
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
    ScanResultValueReader batch = ScanResultValueReader.of(scanValue);
    validateBatch(batch);
  }

  private void validateBatch(ScanResultValueReader batch)
  {
    assertEquals(2, batch.size());

    // Check inferred schema
    RowSchema expected = new SchemaBuilder()
        .scalar("l", ColumnType.LONG)
        .scalar("s", ColumnType.STRING)
        .scalar("f", ColumnType.FLOAT)
        .scalar("d", ColumnType.DOUBLE)
        .scalar("o", ColumnType.UNKNOWN_COMPLEX)
        .scalar("n", null)
        .build();
    assertEquals(expected, batch.schema());
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
    ScanResultValueReader batch = ScanResultValueReader.of(scanValue);
    validateBatch(batch);
  }
}
