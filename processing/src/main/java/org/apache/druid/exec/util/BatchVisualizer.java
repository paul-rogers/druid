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

import io.netty.util.SuppressForbidden;
import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.exec.batch.Batch;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.column.ColumnType;

/**
 * Convert a batch to a CSV-like format for debugging.
 */
public class BatchVisualizer
{
  private static final Logger LOG = new Logger(BatchVisualizer.class);

  public static String visualize(Batch batch)
  {
    StringBuilder buf = new StringBuilder();
    visualizeSchema(batch.schema().rowSchema(), buf);
    visualizeRows(batch.newCursor(), buf);
    return buf.toString();
  }

  @SuppressForbidden(reason = "System#out")
  public static void print(Batch batch)
  {
    System.out.println(visualize(batch));
  }

  public static void log(Batch batch)
  {
    log(batch, LOG);
  }

  public static void log(Batch batch, Logger log)
  {
    log.debug(visualize(batch));
  }

  private static void visualizeSchema(RowSchema schema, StringBuilder buf)
  {
    for (int i = 0; i < schema.size(); i++) {
      ColumnSchema col = schema.column(i);
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(col.name());
      if (col.type() != null) {
        buf.append(" (")
           .append(col.type().asTypeString())
           .append(")");
      }
    }
    buf.append("\n");
  }

  private static void visualizeRows(BatchCursor cursor, StringBuilder buf)
  {
    int row = 0;
    while (cursor.sequencer().next()) {
      row++;
      buf.append(StringUtils.format("%4d: ", row));
      visualizeRow(cursor, buf);
    }
  }

  private static void visualizeRow(BatchCursor cursor, StringBuilder buf)
  {
    for (int i = 0; i < cursor.columns().schema().size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      ScalarColumnReader col = cursor.columns().scalar(i);
      if (col.isNull()) {
        buf.append("null");
        continue;
      }
      ColumnType type = col.schema().type();
      if (type == ColumnType.STRING) {
        buf.append("\"").append(col.getString()).append("\"");
      } else {
        buf.append(col.getValue().toString());
      }
    }
    buf.append("\n");
  }
}
