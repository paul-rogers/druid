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

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchReader.BatchCursor;
import org.apache.druid.exec.batch.ColumnReaderFactory;
import org.apache.druid.exec.batch.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.segment.column.ColumnType;

import java.util.Objects;

/**
 * Compares two batches of data, primarily for testing and
 * debugging. There is no requirement that the batches share the same
 * underlying data representation: the test is at the level of the
 * schema and the column values.
 * <p>
 * The version here returns a Boolean value. See {@code BatchValidator}
 * in the test package for a version that uses JUnit assertions an
 * displays an explanation of the difference.
 */
public class BatchEquality
{
  public interface ErrorReporter
  {
    void error(String msg, Object...args);
  }

  private final ErrorReporter reporter;

  public BatchEquality(ErrorReporter reporter)
  {
    this.reporter = reporter;
  }

  public static BatchEquality simple()
  {
    return new BatchEquality((error, args) -> {});
  }

  public static boolean equals(Batch batch1, Batch batch2)
  {
    return simple().isEqual(batch1, batch2);
  }

  public boolean isEqual(Batch batch1, Batch batch2)
  {
    if (batch1 == null && batch2 == null) {
      return true;
    }
    if (batch1 == null) {
      reporter.error("Batch 1 is null but Batch 2 is not");
      return false;
    }
    if (batch2 == null) {
      reporter.error("Batch 2 is null but Batch 1 is not");
      return false;
    }
    return simple().isContentEqual(batch1.newReader(), batch2.newReader());
  }

  public static boolean equals(BatchReader batch1, BatchReader batch2)
  {
    return simple().isEqual(batch1, batch2);
  }

  public boolean isEqual(BatchReader batch1, BatchReader batch2)
  {
    if (batch1 == null && batch2 == null) {
      return true;
    }
    if (batch1 == null) {
      reporter.error("Batch 1 is null but Batch 2 is not");
      return false;
    }
    if (batch2 == null) {
      reporter.error("Batch 2 is null but Batch 1 is not");
      return false;
    }
    return isContentEqual(batch1, batch2);
  }

  public boolean isContentEqual(BatchReader batch1, BatchReader batch2)
  {
    ColumnReaderFactory reader1 = batch1.columns();
    ColumnReaderFactory reader2 = batch2.columns();
    if (!reader1.schema().equals(reader2.schema())) {
      reporter.error("Schemas are different");
      return false;
    }
    BatchCursor cursor1 = batch1.batchCursor();
    BatchCursor cursor2 = batch2.batchCursor();
    while (true) {
      boolean ok1 = cursor1.next();
      boolean ok2 = cursor2.next();
      if (!ok1 && !ok2) {
        return true;
      }
      int row = cursor1.index() + 1;
      if (!ok1) {
        reporter.error("EOF on batch 1 at row %d, batch 2 has more rows", row);
        return false;
      }
      if (!ok2) {
        reporter.error("EOF on batch 2 at row %d, batch 1 has more rows", row);
        return false;
      }
      if (!isRowEqual(row, reader1, reader2)) {
        return false;
      }
    }
  }

  public static boolean equals(ColumnReaderFactory reader1, ColumnReaderFactory reader2)
  {
    return simple().isEqual(reader1, reader2);
  }

  public boolean isEqual(ColumnReaderFactory reader1, ColumnReaderFactory reader2)
  {
    if (reader1 == null && reader2 == null) {
      return true;
    }
    if (reader1 == null || reader2 == null) {
      return false;
    }
    if (!reader1.schema().equals(reader2.schema())) {
      return false;
    }
    return isRowEqual(0, reader1, reader2);
  }

  public boolean isRowEqual(int row, ColumnReaderFactory reader1, ColumnReaderFactory reader2)
  {
    for (int col = 0; col < reader1.schema().size(); col++) {
      ScalarColumnReader colReader1 = reader1.scalar(col);
      ScalarColumnReader colReader2 = reader2.scalar(col);
      boolean null1 = colReader1.isNull();
      boolean null2 = colReader2.isNull();
      if (null1 && null2) {
        continue;
      }
      if (null1) {
        reporter.error(
            "Row %d, column %d [%s] batch 1 is null, but batch 2 is not",
            row,
            col + 1,
            reader1.schema().column(col).name()
        );
        return false;
      }
      if (null2) {
        reporter.error(
            "Row %d, column %d [%s] batch 2 is null, but batch 1 is not",
            row,
            col + 1,
            reader1.schema().column(col).name()
        );
        return false;
      }
      ColumnType colType = reader1.schema().column(col).type();
      if (colType == null) {
        colType = ColumnType.UNKNOWN_COMPLEX;
      }
      if (colType == ColumnType.FLOAT || colType == ColumnType.DOUBLE) {
        double value1 = colReader1.getDouble();
        double value2 = colReader2.getDouble();
        if (value1 == value2) {
          continue;
        }

        // Compute the error as a ratio of the value. This is more stable
        // than the JUnit simple difference and allows us to hard-code the delta.
        double diff = Math.abs(value1 - value2);
        double err = Math.abs(diff / value1);
        if (err < 0.001) {
          continue;
        }
        reporter.error(
            "Row %d, column %d [%s], values differ: [%f] and [%f]",
            row,
            col + 1,
            reader1.schema().column(col).name(),
            value1,
            value2
        );
        return false;
      }
      if (!Objects.equals(colReader1.getValue(), colReader2.getValue())) {
        reporter.error(
            "Row %d, column %d [%s], values differ: [%s] and [%s]",
            row,
            col + 1,
            reader1.schema().column(col).name(),
            colReader1.getValue(),
            colReader2.getValue()
        );
        return false;
      }
    }
    return true;
  }
}
