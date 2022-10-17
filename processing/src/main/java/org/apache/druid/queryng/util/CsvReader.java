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

package org.apache.druid.queryng.util;

import org.apache.druid.data.input.InputEntity;
import org.apache.druid.data.input.InputEntityReader;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.ResultIterator;
import org.apache.druid.queryng.rows.BatchWriter;
import org.apache.druid.queryng.rows.RowWriter;
import org.apache.druid.queryng.rows.SchemaBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CsvReader<T> implements IterableOperator<T>
{
  private final FragmentContext context;
  private final CsvInputFormat inputFormat;
  private final InputRowSchema inputRowSchema;
  private final InputEntity source;
  private final File temporaryDirectory;
  private final BatchWriter<T> batchWriter;
  private CloseableIterator<InputRow> inputIter;
  private int batchCount;
  private int rowCount;

  public CsvReader(
      FragmentContext context,
      CsvInputFormat inputFormat,
      InputRowSchema inputRowSchema,
      InputEntity source,
      File temporaryDirectory,
      BatchWriter.BatchWriterFactory<T> batchWriterFactory
  )
  {
    this.context = context;
    this.inputFormat = inputFormat;
    this.inputRowSchema = inputRowSchema;
    this.source = source;
    this.temporaryDirectory = temporaryDirectory;
    SchemaBuilder schemaBuilder = new SchemaBuilder();
    schemaBuilder.scalar(ColumnHolder.TIME_COLUMN_NAME, ColumnType.LONG);
    for (String col : inputRowSchema.getDimensionsSpec().getDimensionNames()) {
      schemaBuilder.scalar(col, ColumnType.STRING);
    }
    this.batchWriter = batchWriterFactory.create(schemaBuilder.build());
  }

  @Override
  public ResultIterator<T> open()
  {
    InputEntityReader entityReader = inputFormat.createReader(inputRowSchema, source, temporaryDirectory);
    try {
      inputIter = entityReader.read();
    }
    catch (IOException e) {
      inputIter = null;
      throw new RE(e, "Failed to open the CSV input source");
    }
    return this;
  }

  @Override
  public T next() throws EofException
  {
    if (inputIter == null) {
      throw Operators.eof();
    }
    int startCount = rowCount;
    RowWriter rowWriter = batchWriter.row();
    while (inputIter.hasNext()) {
      if (!batchWriter.newRow()) {
        break;
      }
      rowCount++;
      InputRow row = inputIter.next();
      rowWriter.scalar(0).setLong(row.getTimestampFromEpoch());
      List<String> dims = row.getDimensions();
      for (int i = 0; i < dims.size(); i++) {
        rowWriter.scalar(i + 1).setString(dims.get(i));
      }
    }
    if (rowCount == startCount) {
      throw Operators.eof();
    }
    batchCount++;
    return batchWriter.harvest();
  }

  @Override
  public void close(boolean cascade)
  {
    if (inputIter == null) {
      return;
    }
    try {
      inputIter.close();
    }
    catch (IOException e) {
      // Ignore
    }
    inputIter = null;
    OperatorProfile profile = new OperatorProfile("CSV Reader");
    profile.add(OperatorProfile.BATCH_COUNT_METRIC, batchCount);
    profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
    context.updateProfile(this, profile);
  }
}
