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

import org.apache.druid.queryng.rows.RowSchema.ColumnSchema;

public class Batches
{
  /**
   * Trivial row reader for an empty batch: no rows and no columns.
   * Probably not possible in practice, but covers the occasional odd
   * case which is perfectly valid in SQL.
   */
  public static class EmptyRowReader extends AbstractRowReader<Void>
  {
    private class ColumnReaderImpl extends AbstractScalarReader
    {
      private int index;

      public ColumnReaderImpl(int index)
      {
        this.index = index;
      }

      @Override
      public ColumnSchema schema()
      {
        return EmptyRowReader.this.schema().column(index);
      }

      @Override
      public Object getObject()
      {
        return null;
      }
    }

    public EmptyRowReader(final RowSchema schema)
    {
      super(schema, () -> null);
    }

    @Override
    protected ScalarColumnReader makeColumn(int ordinal)
    {
      return new ColumnReaderImpl(ordinal);
    }
  }

  /**
   * Trivial reader for an empty batch. Most useful when the format of the batch
   * is unknown because every format behaves the same when empty.
   */
  public static class EmptyBatchReader<T> extends AbstractBatchReader<T>
  {
    public EmptyBatchReader(RowSchema schema)
    {
      super(schema);
      this.rowReader = new EmptyRowReader(schema);
    }

    @Override
    public int size()
    {
      return 0;
    }
  }

  public static <T> BatchReader<T> emptyBatchReader(RowSchema schema)
  {
    return new EmptyBatchReader<T>(schema);
  }

  public static class EmptyListReader<T> extends ListReader<T>
  {
    public EmptyListReader(RowSchema schema)
    {
      super(schema);
    }

    @Override
    public int size()
    {
      return 0;
    }
  }

  public static <T> ListReader<T> emptyListReader(RowSchema schema)
  {
    return new EmptyListReader<T>(schema);
  }

}
