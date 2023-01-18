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

import java.util.Map;
import java.util.function.Supplier;

class MapReader extends AbstractRowReader<Map<String, Object>>
{
  class ColumnReaderImpl extends AbstractScalarReader
  {
    private final String name;

    public ColumnReaderImpl(String name)
    {
      this.name = name;
    }

    @Override
    public Object getObject()
    {
      return row.get(name);
    }

    @Override
    public ColumnSchema schema()
    {
      return MapReader.this.schema().column(name);
    }
  }

  public MapReader(final RowSchema schema, final Supplier<Map<String, Object>> rowSupplier)
  {
    super(schema, rowSupplier);
  }

  @Override
  protected ScalarColumnReader makeColumn(int ordinal)
  {
    return new ColumnReaderImpl(schema().column(ordinal).name());
  }
}
