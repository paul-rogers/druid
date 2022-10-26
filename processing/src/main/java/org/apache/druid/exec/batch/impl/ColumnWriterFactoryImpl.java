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

import org.apache.druid.exec.batch.ColumnWriterFactory;
import org.apache.druid.exec.batch.RowSchema;

/**
 * Provides column writers for a batch writer. Writers are created at
 * the start: typically an operator that writes a batch must write all the
 * columns of that batch.
 */
public class ColumnWriterFactoryImpl implements ColumnWriterFactory
{
  protected final RowSchema schema;
  protected final ScalarColumnWriter[] columnWriters;

  public ColumnWriterFactoryImpl(
      final RowSchema schema,
      final ScalarColumnWriter[] columnWriters
  )
  {
    this.schema = schema;
    this.columnWriters = columnWriters;
  }

  @Override
  public RowSchema schema()
  {
    return schema;
  }

  @Override
  public ScalarColumnWriter scalar(String name)
  {
    int index = schema.ordinal(name);
    return index < 0 ? null : scalar(index);
  }

  @Override
  public ScalarColumnWriter scalar(int ordinal)
  {
    return columnWriters[ordinal];
  }
}
