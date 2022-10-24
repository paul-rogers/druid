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

package org.apache.druid.exec.operator;

import org.apache.druid.exec.batch.impl.IndirectBatch;
import org.apache.druid.exec.operator.impl.RowSchemaImpl;
import org.apache.druid.exec.util.BatchCopier;
import org.apache.druid.exec.util.BatchCopierFactory;
import org.apache.druid.java.util.common.UOE;

public class Batches
{
  public static Batch indirectBatch(Batch results, int[] index)
  {
    return new IndirectBatch(results, index);
  }

  /**
   * Optimized copy of rows from one batch to another. To fully optimize, ensure
   * the same reader and writer are used across batches to avoid the need to
   */
  public static BatchCopier copier(BatchReader source, BatchWriter dest)
  {
    return BatchCopierFactory.build(source, dest);
  }

  /**
   * Convenience, non-optimized method to copy a all rows between batches
   * with compatible schemas. Consider {@link BatchCopier}, obtained from
   * {@link #copier(BatchReader, BatchWriter)}, for production use.
   */
  public static boolean copy(BatchReader source, BatchWriter dest)
  {
    while (source.cursor().next()) {
      if (!copyRow(source, dest)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Convenience, non-optimized method to copy a row between batches
   * with compatible schemas. Consider {@link org.apache.druid.exec.util.BatchCopier}
   * for production use.
   */
  private static boolean copyRow(BatchReader source, BatchWriter dest)
  {
    ColumnReaderFactory sourceColumns = source.columns();
    ColumnWriterFactory destColumns = dest.columns();
    int columnCount = sourceColumns.schema().size();

    // Quick & dirty check on the number of columns. We trust that
    // the caller has ensured the types match or are compatible.
    if (destColumns.schema().size() != columnCount) {
      throw new UOE("Cannot copy rows between differing schemas: use a projection");
    }
    for (int i = 0; i < columnCount; i++) {
      destColumns.scalar(i).setValue(sourceColumns.scalar(i).getValue());
    }
    return false;
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
    return new IndirectBatch(batch, index);
  }
}
