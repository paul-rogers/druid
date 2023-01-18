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

package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.segment.column.ColumnType;

public abstract class AbstractScalarReader implements ScalarColumnReader
{
  @Override
  public boolean isNull()
  {
    return getObject() == null;
  }

  @Override
  public String getString()
  {
    Object value = getObject();
    return value == null ? null : value.toString();
  }

  @Override
  public long getLong()
  {
    Object value = getObject();
    if (value instanceof Integer) {
      return (Integer) value;
    } else {
      return (Long) value;
    }
  }

  @Override
  public double getDouble()
  {
    Object value = getObject();
    if (value instanceof Long) {
      return (Long) value;
    } else if (value instanceof Float) {
      return (Float) value;
    } else {
      return (Double) value;
    }
  }

  @Override
  public Object getValue()
  {
    ColumnType colType = schema().type();
    if (colType == null) {
      colType = ColumnType.UNKNOWN_COMPLEX;
    }
    if (colType == ColumnType.STRING) {
      return getString();
    } else if (colType == ColumnType.LONG) {
      return getLong();
    } else if (colType == ColumnType.FLOAT || colType == ColumnType.DOUBLE) {
      return getDouble();
    } else {
      return getObject();
    }
  }
}
