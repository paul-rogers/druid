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

package org.apache.druid.exec.operator;

/**
 * Reader for a batch of rows. There are two ways to work with a batch: sequentially
 * or randomly. To read sequentially, call {@link #reset()}, then call {@link #next()}
 * to move to the first row. {@code reset()} moves the position before the first row.
 * <p>
 * To read randomly, call {@link #size()} to determine the row count, then call
 * {@link #seek(int)} to move to a specific row.
 * <p>
 * Call {@link #bind(Object)} to attach the reader to the actual data batch. Calling
 * {@code bind()} does an implicit {@code reset()}.
 * <p>
 * Call {@link #row()} to obtain the row reader for a row. The row reader is the same
 * for all rows: the batch reader positions the row reader automatically. Client code
 * can cache the row reader, or fetch it each time it is needed.
 */
public interface BatchReader
{
  public interface BatchCursor
  {
    int size();

    /**
     * Position the reader before the first row of the batch, so that
     * the next call to {@link #next()} moves to the first row.
     */
    void reset();
    int index();

    /**
     * Seek to the given position. If the position is -1, the behavior is the
     * same as {@link #reset()}: position before the first row. If the position is
     * {@link #size()}, then the position is after the last row. Else, the
     * batch is positioned at the requested location.
     * @return {@code true} if the requested position is readable, {@code false}
     * if the position is before the start or after the end.
     */
    boolean seek(int posn);

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
  }

  ColumnReaderFactory columns();
  BatchCursor cursor();
  <T> T unwrap(Class<T> readerClass);
}
