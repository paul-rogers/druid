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

package org.apache.druid.exec.batch;

/**
 * Reader for a possibly unbounded set of rows with a common schema and common
 * set of column readers, where "unbounded" simply means that the total number
 * of rows is not necessarily known. A "reader" differs from an iterator: a
 * reader does not deliver a row, but rather positions a set of column readers
 * on the current row. Reading is done using the column readers. The result
 * isolates the client from the implementation details of the underlying data
 * set which may be materialized, streaming, or some combination.
 */
public interface RowCursor
{
  /**
   * Read a set of rows sequentially. Call {@link #next()}
   * to move to the first row. The cursor starts before the position before the first row,
   * and ends beyond the position of the last row.
   */
  interface RowSequencer
  {
    /**
     * Move to the next row, if any. At EOF, the position points past the
     * last row.
     *
     * @return {@code true} if there is a row to read, {@code false} if EOF.
     */
    boolean next();

    /**
     * Report if the reader is positioned past the end of the last row (i.e. EOF).
     * This call is not normally needed: the call to {@ink #next()} reports the
     * same information.
     */
    boolean isEOF();

    /**
     * Report if the reader is positioned on a valid data row.
     *
     * @return {true} if a data row is available, {@false} if the cursor is
     * positioned before the first or after the last row, and so no row is
     * available.
     */
    boolean isValid();
  }

  ColumnReaderProvider columns();
  RowSequencer sequencer();
}
