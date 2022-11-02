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

package org.apache.druid.exec.batch;

import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.impl.BatchImpl;
import org.apache.druid.exec.batch.impl.IndirectBatchType;
import org.apache.druid.exec.shim.MapListBatchType;
import org.apache.druid.exec.shim.ObjectArrayListBatchType;
import org.apache.druid.exec.shim.ScanResultValueBatchType;

import java.util.List;

public class Batches
{
  /**
   * Convenience, non-optimized method to copy a all rows between batches
   * with compatible schemas. Consider {@link BatchCopier}, obtained from
   * {@link #copier(BatchCursor, BatchWriter)}, for production use.
   */
  public static boolean copy(BatchCursor source, BatchWriter<?> dest)
  {
    dest.copier(source).copy(Integer.MAX_VALUE);
    return source.sequencer().isEOF();
  }

  public static RowSchema emptySchema()
  {
    return RowSchemaImpl.EMPTY_SCHEMA;
  }

  public static Batch reverseOf(Batch batch)
  {
    int n = batch.size();
    if (n < 2) {
      return batch;
    }
    final int[] index = new int[n];
    for (int i = 0; i < n; i++) {
      index[i] = n - i - 1;
    }
    return indirectBatch(batch, index);
  }

  public static Batch indirectBatch(Batch batch, int[] index)
  {
    return of(
        IndirectBatchType.of(batch.schema().type()),
        batch.schema().rowSchema(),
        IndirectBatchType.wrap(batch.data(), index)
    );
  }

  public static Batch of(BatchType type, RowSchema schema, Object data)
  {
    return of(type.batchSchema(schema), data);
  }

  public static Batch of(BatchSchema schema, Object data)
  {
    return new BatchImpl(schema, data);
  }

  public static ScalarColumnReader[] readProjection(BatchCursor cursor, List<String> cols)
  {
    ScalarColumnReader[] readers = new ScalarColumnReader[cols.size()];
    for (int i = 0; i < readers.length; i++) {
      readers[i] = cursor.columns().scalar(cols.get(i));
    }
    return readers;
  }

  public static BatchType typeFor(BatchFormat format)
  {
    switch (format) {
      case OBJECT_ARRAY:
        return ObjectArrayListBatchType.INSTANCE;
      case MAP:
        return MapListBatchType.INSTANCE;
      case SCAN_OBJECT_ARRAY:
        return ScanResultValueBatchType.ARRAY_INSTANCE;
       case SCAN_MAP:
         return ScanResultValueBatchType.MAP_INSTANCE;
      default:
        return null;
    }
  }

  public static boolean canDirectCopy(BatchCursor cursor, BatchWriter<?> writer)
  {
    return writer.schema().type().canDirectCopyFrom(cursor.schema().type());
  }
}
