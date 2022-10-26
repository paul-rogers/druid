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

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.test.TestUtils;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CopierTest
{
  public boolean directCopyable(BatchFormat from, BatchFormat to)
  {
    if (from == to) {
      return true;
    }
    switch (from) {
      case OBJECT_ARRAY:
        return to == BatchFormat.SCAN_OBJECT_ARRAY;
      case MAP:
        return to == BatchFormat.SCAN_MAP;
      case SCAN_OBJECT_ARRAY:
        return to == BatchFormat.OBJECT_ARRAY;
      case SCAN_MAP:
        return to == BatchFormat.MAP;
      default:
        return false;
    }
  }

  @Test
  public void testCopy()
  {
    for (BatchFormat sourceFormat : BatchFormat.values()) {
      for (BatchFormat destFormat : BatchFormat.values()) {
        doCopyTest(sourceFormat, destFormat);
      }
    }
  }

  private void doCopyTest(BatchFormat sourceFormat, BatchFormat destFormat)
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("a", ColumnType.STRING)
        .scalar("b", ColumnType.LONG)
        .build();

    BatchWriter<?> destWriter = TestUtils.writerFor(schema, destFormat, 6);
    destWriter.newBatch();

    // Create a batch and verify the correct copier kind is created.
    Batch sourceBatch = TestUtils.builderFor(schema, sourceFormat)
        .row("first", 1)
        .row("second", 2)
        .build();
    BatchReader sourceReader = sourceBatch.newReader();

    BatchCopier copier = Batches.copier(sourceReader, destWriter);
    if (directCopyable(sourceFormat, destFormat)) {
      assertTrue(copier instanceof BatchCopierFactory.DirectCopier);
    } else {
      assertTrue(copier instanceof BatchCopierFactory.GenericCopier);
    }

    // Copy the first batch row-by-row
    assertTrue(copier.copyRow(sourceReader, destWriter));
    assertTrue(copier.copyRow(sourceReader, destWriter));
    assertFalse(copier.copyRow(sourceReader, destWriter));

    // Copy the second batch in bulk.
    sourceBatch = TestUtils.builderFor(schema, sourceFormat)
        .row("third", 3)
        .row("fourth", 4)
        .build();
    sourceBatch.bindReader(sourceReader);
    assertTrue(copier.copyAll(sourceReader, destWriter));

    // Copy the third batch in bulk, but only 2 of the 3 rows fit.
    sourceBatch = TestUtils.builderFor(schema, sourceFormat)
        .row("fifth", 5)
        .row("sixth", 6)
        .row("seventh", 7)
        .build();
    sourceBatch.bindReader(sourceReader);
    assertFalse(copier.copyAll(sourceReader, destWriter));

    // Verify the combined result.
    Batch expected = TestUtils.builderFor(schema, sourceFormat)
        .row("first", 1)
        .row("second", 2)
        .row("third", 3)
        .row("fourth", 4)
        .row("fifth", 5)
        .row("sixth", 6)
        .build();
    BatchValidator.assertEquals(expected, destWriter.harvestAsBatch());
  }
}
