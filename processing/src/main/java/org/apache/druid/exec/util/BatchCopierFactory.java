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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchReader.BatchCursor;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.ColumnReaderFactory;
import org.apache.druid.exec.operator.ColumnWriterFactory;
import org.apache.druid.java.util.common.UOE;

public class BatchCopierFactory
{
  /**
   * Generic copier used when the source and destination formats differ.
   * The common denominators are the column accessors (readers and writers)
   * which can be used to bridge formats.
   */
  @VisibleForTesting
  protected static class GenericCopier implements BatchCopier
  {
    private final ColumnCopier[] columns;

    public GenericCopier(ColumnReaderFactory sourceColumns, ColumnWriterFactory destColumns)
    {
      int columnCount = sourceColumns.schema().size();

      // Quick & dirty check on the number of columns. We trust that
      // the caller has ensured the types match or are compatible.
      if (destColumns.schema().size() != columnCount) {
        throw new UOE("Cannot copy rows between differing schemas: use a projection");
      }
      columns = new ColumnCopier[columnCount];
      for (int i = 0; i < columnCount; i++) {
        columns[i] = ColumnCopierFactory.build(sourceColumns.scalar(i), destColumns.scalar(i));
      }
    }

    @Override
    public boolean copyAll(BatchReader source, BatchWriter dest)
    {
      return copyRows(source, dest, Integer.MAX_VALUE);
    }

    @Override
    public boolean copyRow(BatchReader source, BatchWriter dest)
    {
      return copyRows(source, dest, 1);
    }

    public boolean copyRows(BatchReader source, BatchWriter dest, int n)
    {
      BatchCursor sourceCursor = source.cursor();
      for (int i = 0; i < n; i++) {
        if (!sourceCursor.next()) {

          // Return true if we copied all rows as we wanted,
          // false if we copied less than the desired number otherwise.
          return n == Integer.MAX_VALUE;
        }
        if (!dest.newRow()) {
          sourceCursor.seek(sourceCursor.index() - 1);
          return false;
        }
        copyRow();
      }
      return true;
    }

    private void copyRow()
    {
      for (int i = 0; i < columns.length; i++) {
        columns[i].copy();
      }
    }
  }

  /**
   * Direct copy when the implementations are compatible and rows can be
   * copied at a lower level by the writer itself.
   */
  @VisibleForTesting
  protected static class DirectCopier implements BatchCopier
  {
    @Override
    public boolean copyAll(BatchReader source, BatchWriter dest)
    {
      dest.directCopy(source, Integer.MAX_VALUE);
      return source.cursor().isEOF();
    }

    @Override
    public boolean copyRow(BatchReader source, BatchWriter dest)
    {
      int start = dest.size();
      dest.directCopy(source, 1);
      return dest.size() > start;
    }
  }

  private static final BatchCopier DIRECT_COPY = new DirectCopier();

  public static BatchCopier build(BatchReader source, BatchWriter dest)
  {
    if (dest.canDirectCopyFrom(source)) {
      return DIRECT_COPY;
    } else {
      return new GenericCopier(source.columns(), dest.columns());
    }
  }
}
