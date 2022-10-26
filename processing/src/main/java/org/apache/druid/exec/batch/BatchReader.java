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

import org.apache.druid.exec.util.ExecUtils;
import org.apache.druid.java.util.common.UOE;

/**
 * Reader for a batch of rows.
 * <p>
 * Call {@link #bind(Object)} to attach the reader to the actual data batch. Calling
 * {@code bind()} does an implicit {@code reset()}.
 * <p>
 * Call {@link #row()} to obtain the row reader for a row. The row reader is the same
 * for all rows: the batch reader positions the row reader automatically. Client code
 * can cache the row reader, or fetch it each time it is needed.
 */
public interface BatchReader extends RowReader
{
  /**
   * Cursor for a batch of rows. There are two ways to work with a batch: sequentially
   * (via the base interface {@link RowCursor} methods), or or randomly as defined
   * here. This class adds the ability to make multiple sequential passes over a
   * batch: call {@link #reset()} to start another pass.
   * <p>
   * To read randomly, call {@link #size()} to determine the row count, then call
   * {@link #seek(int)} to move to a specific row.
   */
  public interface BatchCursor extends RowCursor
  {
    /**
     * Position the reader before the first row of the batch, so that
     * the next call to {@link #next()} moves to the first row.
     *
     * @throws UOE if this is a one-pass cursor and cannot be reset.
     * Usually known from context. See the batch capabilities for a
     * dynamic indication.
     */
    void reset();

    int index();

    int size();

    /**
     * Seek to the given position. If the position is -1, the behavior is the
     * same as {@link #reset()}: position before the first row. If the position is
     * {@link #size()}, then the position is after the last row. Else, the
     * batch is positioned at the requested location.
     * @return {@code true} if the requested position is readable, {@code false}
     * if the position is before the start or after the end.
     */
    boolean seek(int posn);
  }

  BatchFactory factory();
  BatchCursor batchCursor();

  default <T> T unwrap(Class<T> readerClass)
  {
    return ExecUtils.unwrap(this, readerClass);
  }
}
