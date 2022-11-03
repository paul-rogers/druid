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

import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;

import java.util.List;

/**
 * Creates and writes to a batch. Call {@link #newBatch()} to create a new
 * batch for writing. Call {@link #harvest()} to "harvest" the completed
 * batch.
 * <p>
 * Batches are written sequentially using a "pull" model. To write, the
 * application creates either a {@link RowWriter} or a {@link Copier}.
 * The {@code RowWriter} allows an arbitrary set of column readers to populate
 * the row: the only restriction is that there must be one column reader for
 * each target column. The {@code Copier} is for the special case of copying
 * between identical rows, and may be optimized internally.
 * <p>
 * Batches have a size limit. {@link #isFull()} returns whether the
 * batch is full. The {@link RowWriter} and {@link Copier} interfaces also
 * observe the limit.
 */
public interface BatchWriter<T>
{
  /**
   * Writer for an individual row. Data comes from a set of projections
   * provided at creation time. Advances the position of the writer,
   * but not the reader: the caller is responsible for the cursor's positioner.
   */
  public interface RowWriter
  {
    /**
     * Write a row by projecting columns from the projector provided at
     * creation time.
     *
     * @return {@code true} if a new row was created, {@code false} if the batch is
     * full and no new rows can be added. If full, harvest the current batch,
     * create a new batch, and again write a row with the same projections.
     */
    boolean write();
  }

  /**
   * Copies any number of rows from a reader provided at creation time
   * to this batch. The reader and writer schemas must be the same. Writers
   * will optimize this path if the reader and writer are compatible and allow
   * efficient row copies. Advances both the reader and writer positions.
   *
   * @return the number of rows copied which is the lesser of the number of
   * rows available from the input, space available in the target batch, or
   * the number of rows requested. If the batch fills, the cursor positioner is
   * left positioned at the last row copied so that a second copy into the
   * new batch resumes where this one left off.
   */
  public interface Copier
  {
    boolean copyRow();
    int copy(BatchPositioner positioner, int n);
  }

  BatchSchema schema();

  RowWriter rowWriter(List<ScalarColumnReader> readers);
  Copier copier(BatchReader source);

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
}
