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

package org.apache.druid.queryng.rows;

/**
 * Creates and writes to a batch. Call {@link #newBatch()} to create a new
 * batch for writing. Call {@link #harvest()} to "harvest" the completed
 * batch.
 * <p>
 * Batches are written sequentially. Call {@link #newRow()} to create each
 * new row. Batches can have a size limit. {@code newRow()} will return
 * {@code false} if the batch is full. {@link #isFull()} returns the same
 * information.
 * <p>
 * Each row of the batch is written using a {@link RowWriter} provided by
 * {@link #row()}. The same row writer is used for all batches. The client can
 * cache the row writer, or ask for it each time it is needed.
 */
public interface BatchWriter<T> extends Batch
{
  interface BatchWriterFactory<T>
  {
    BatchWriter<T> create(RowSchema schema);
  }

  /**
   * Create a new batch. Must be called before writing to the batch, and
   * after a call to {@link #harvest()}, if you want to create another batch.
   */
  void newBatch();

  /**
   * Returns the current size. Mostly for information purposes: rely on the
   * size limit to limit batch size.
   * @return
   */
  int size();

  /**
   * @return {@code true} if the batch has reached its size limit, {@code false} if
   * the batch will accept more rows.
   */
  boolean isFull();

  /**
   * Create a new row. Call this before writing to a row.
   *
   * @return {@code true} if a new row was created, {@code false} if the batch is
   * full and no new rows can be added.
   */
  boolean newRow();

  /**
   * @return the {@link RowWriter} for this batch. All rows use the same
   * row writer.
   */
  RowWriter row();

  /**
   * Retrieve the completed batch. Moves the batch writer into an idle state:
   * no writing operations are allowed until {@link #newBatch()} is called again.
   * @return the newly created batch
   */
  T harvest();

  /**
   * Harvest the current batch and return it wrapped in a batch reader. Allows
   * for generic code that never touches the concrete batch implementation.
   * @return
   */
  BatchReader<T> toReader();
}
