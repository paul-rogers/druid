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
  int size();
  void bindListener(BindingListener listener);
  void updatePosition(int posn);

  default <T> T unwrap(Class<T> readerClass)
  {
    return ExecUtils.unwrap(this, readerClass);
  }
}
