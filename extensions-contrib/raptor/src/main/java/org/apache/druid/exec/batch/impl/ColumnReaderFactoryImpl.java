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

import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.RowSchema;

import java.util.ArrayList;
import java.util.List;

/**
 * Provides column readers for a batch reader. Readers are created lazily, then
 * cached. Lazy creation is used because may operators use a subset of columns:
 * no need to create all the column readers in that case.
 */
public class ColumnReaderFactoryImpl implements ColumnReaderProvider
{
  public interface ColumnReaderMaker
  {
    ScalarColumnReader buildReader(int index);
  }

  private final RowSchema schema;
  private final ColumnReaderMaker readerMaker;
  private final ScalarColumnReader[] columnReaders;

  public ColumnReaderFactoryImpl(final RowSchema schema, final ColumnReaderMaker readerMaker)
  {
    this.schema = schema;
    columnReaders = new ScalarColumnReader[schema.size()];
    this.readerMaker = readerMaker;
  }

  @Override
  public RowSchema schema()
  {
    return schema;
  }

  @Override
  public ScalarColumnReader scalar(String name)
  {
    int index = schema.ordinal(name);
    return index == -1 ? null : scalar(index);
  }

  @Override
  public ScalarColumnReader scalar(int ordinal)
  {
    if (columnReaders[ordinal] == null) {
      columnReaders[ordinal] = readerMaker.buildReader(ordinal);
    }
    return columnReaders[ordinal];
  }

  @Override
  public List<ScalarColumnReader> columns()
  {
    // Done this way, rather than Arrays.asList(), because we may have
    // to materialize the reader if it has not yet been materialized.
    List<ScalarColumnReader> readers = new ArrayList<>();
    for (int i = 0; i < columnReaders.length; i++) {
      readers.add(scalar(i));
    }
    return readers;
  }
}
