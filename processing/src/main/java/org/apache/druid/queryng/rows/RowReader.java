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

/**
 * Operator equivalent of {@code ColumnSelectorFactory}. Provides read-only
 * access to a column that is presumed to be "dumb": just a bit of data
 * in a data structure. A row reader has a persistent set of column
 * readers.
 */
public interface RowReader
{
  public interface RowIndex
  {
    int index();
  }

  public interface BindableRowReader extends RowReader
  {
    void bind();
  }

  /**
   * Reader for a column. Columns have a type, as given by the {@link ColumnSchema}.
   * But, the reader is generic: one interface for all column types. The only
   * methods which are guaranteed to work are those consistent with the type.
   * Others may allow conversions, depending on the underlying type.
   * <p>
   * This reader does not handle arrays or structures. That can be added with
   * additional complexity.
   */
  interface ScalarColumnReader
  {
    ColumnSchema schema();
    boolean isNull();
    String getString();
    long getLong();
    double getDouble();

    /**
     * Return the value of a complex object. This is not a generic
     * getter, for that use {@link #getValue()}.
     */
    Object getObject();

    /**
     * Return the value of any type, as a Java object. If the
     * column is null, returns {@code null}.
     */
    Object getValue();
  }

  RowSchema schema();
  ScalarColumnReader scalar(String name);
  ScalarColumnReader scalar(int ordinal);
}
