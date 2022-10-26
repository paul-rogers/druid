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
public interface BatchWriter<T>
{
  BatchFactory factory();

  ColumnWriterFactory columns();

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
   * Retrieve the completed batch. Moves the batch writer into an idle state:
   * no writing operations are allowed until {@link #newBatch()} is called again.
   * @return the newly created batch
   */
  T harvest();

  /**
   * Wrap the newly created batch in the {@link Batch} interface for generic
   * processing.
   *
   * @return the newly created batch as a {@link Batch}
   */
  Batch harvestAsBatch();

  /**
   * Copies rows from the source into this batch up to the size limit, starting
   * with the current reader position. The reader is left positioned after
   * the last row copied. Only applies when the reader is compatible, as
   * indicated by {@link #canDirectCopyFrom(BatchReader)}. Use
   * {@link org.apache.druid.exec.batch.Batches#copier(BatchReader, BatchWriter)}
   * for a generic solution.
   * <p>
   * Copies up to the given number of rows, the available reader rows, or the
   * capacity of the writer, whichever is lower.
   *
   * @return the number of rows copied
   */
  int directCopy(BatchReader from, int n);
}
