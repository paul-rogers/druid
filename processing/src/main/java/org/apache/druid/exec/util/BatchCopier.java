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

import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;

/**
 * Optimized copy of rows from one batch to another. Use this when the source
 * and destination schemas are identical.
 */
public interface BatchCopier
{
  /**
   * Copy all rows from the source to the destination. Stops after the last
   * source row, or when the destination becomes full.
   *
   * @return {@code true} if all rows are copied and the source is at EOF.
   * {@code false} if the destination filled and not all source rows were
   * copied
   */
  boolean copyAll(BatchReader source, BatchWriter dest);

  /**
   * Copy a single row. Advances the source reader to the next row, which
   * is copied if space exists in the destination. Leaves the source cursor
   * position unchanged if the row does not fit in the destination. Does
   * nothing if the source is already at EOF.
   *
   * @return {@code true} if the next source row was copied, {@code false}
   * if not (which leaves the source cursor unchanged)
   */
  boolean copyRow(BatchReader source, BatchWriter dest);
}
