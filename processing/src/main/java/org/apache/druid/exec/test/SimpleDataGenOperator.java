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

package org.apache.druid.exec.test;

import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.ColumnWriterFactory;
import org.apache.druid.exec.batch.ColumnWriterFactory.ScalarColumnWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.OperatorProfile;
import org.apache.druid.exec.operator.Operators;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.impl.AbstractOperator;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.Instant;

/**
 * Simple operator to generate data. Recognizes a number of column names,
 * all others will product a null string.
 * <ul>
 * <li>{@code __time}: Long, base at 2022-11-22T10:00:00 plus 1 second per (rid - 1)</li>
 * <li>{@code rid}: Long row ID, starting from 1</li>
 * <li>{@code rand}: Long pseudo-random (actually, mod 7 or rid).</li>
 * <li>{@code str}: String: "Row x" where x is the rid.</li>
 * <li>{@code rot}: String, "Rot x" where x is rid mod 5.</li>
 * </li>
 */
public class SimpleDataGenOperator extends AbstractOperator<Object> implements ResultIterator<Object>
{
  private final long START_TIME = Instant.parse("2022-11-22T10:00:00Z").getMillis();

  private final SimpleDataGenSpec plan;
  private final RowSchema schema;
  private BatchWriter<?> writer;
  private int rowCount;
  private int batchCount;

  public SimpleDataGenOperator(FragmentContext context, SimpleDataGenSpec plan)
  {
    super(context);
    this.plan = plan;
    SchemaBuilder schemaBuilder = new SchemaBuilder();
    for (String col : plan.columns) {
      switch (col) {
        case ColumnHolder.TIME_COLUMN_NAME:
        case "rid":
        case "rand":
          schemaBuilder.scalar(col, ColumnType.LONG);
          break;
        case "str":
        case "str5":
          schemaBuilder.scalar(col, ColumnType.STRING);
          break;
        default:
          schemaBuilder.scalar(col, ColumnType.STRING);
      }
    }
    this.schema = schemaBuilder.build();
  }

  @Override
  public ResultIterator<Object> open()
  {
    writer = TestUtils.writerFor(schema, plan.format, plan.batchSize);
    return this;
  }

  @Override
  public Object next() throws EofException
  {
    if (rowCount == plan.rowCount) {
      throw Operators.eof();
    }
    batchCount++;
    writer.newBatch();
    ColumnWriterFactory columns = writer.columns();
    for (; rowCount < plan.rowCount; rowCount++) {
      if (!writer.newRow()) {
        break;
      }

      // Slow: don't try this at home. Normally these would be cached.
      ScalarColumnWriter colWriter = columns.scalar(ColumnHolder.TIME_COLUMN_NAME);
      if (colWriter != null) {
        colWriter.setLong(START_TIME + rowCount * 1000);
      }
      colWriter = columns.scalar("rid");
      if (colWriter != null) {
        colWriter.setLong(rowCount);
      }
      colWriter = columns.scalar("rand");
      if (colWriter != null) {
        colWriter.setLong(rowCount % 7);
      }
      colWriter = columns.scalar("str");
      if (colWriter != null) {
        colWriter.setString("Row " + rowCount);
      }
      colWriter = columns.scalar("str5");
      if (colWriter != null) {
        colWriter.setString("Rot " + (rowCount % 5));
      }
    }
    return writer.harvest();
  }

  @Override
  public void close(boolean cascade)
  {
    OperatorProfile profile = new OperatorProfile("data-gen");
    profile.add(OperatorProfile.BATCH_COUNT_METRIC, batchCount);
    profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
    context.updateProfile(this, profile);
  }
}
