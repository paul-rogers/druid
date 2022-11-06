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

package org.apache.druid.exec.window;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.ResultIterator.EofException;

import java.util.ArrayList;
import java.util.List;

public class BatchBuffer
{
  /**
   * Factory for the holders for input batches.
   */
  protected final BatchSchema inputSchema;

  /**
   * Upstream source of batches
   */
  private final ResultIterator<Object> inputIter;

  /**
   * Sliding window of retained batches.
   */
  // Array list is not ideal: we want an unbounded circular
  // buffer with access to items in the buffer. Surprisingly,
  // no candidates suggested themselves after a Google search.
  // Building one can come later during optimization.
  private final List<Object> buffer = new ArrayList<>();

  /**
   * Total number of batches read thus far from upstream. The
   * buffer holds the last n of these batches.
   */
  private int batchCount;

  /**
   * If EOF was seen from upstream.
   */
  private boolean eof;

  public BatchBuffer(final BatchSchema inputSchema, final ResultIterator<Object> inputIter)
  {
    this.inputSchema = inputSchema;
    this.inputIter = inputIter;
  }

  public boolean isEof()
  {
    return eof;
  }

  /**
   * Load the next non-empty batch from upstream and adds it to the
   * queue.
   *
   * @param batchToLoad the index of the batch to load. Not needed, but
   * passed in and verified to ensure the cursors stay in sync.
   * @return the batch, else {@code null} at EOF.
   */
  public Object loadBatch(int batchToLoad)
  {
    Preconditions.checkState(batchToLoad == batchCount);
    while (true) {
      Object data;
      try {
        data = inputIter.next();
      } catch (EofException e) {
        eof = true;
        return null;
      }
      int size = inputSchema.type().sizeOf(data);
      if (size > 0) {
        batchCount++;
        buffer.add(data);
        return data;
      }
    }
  }

  @VisibleForTesting
  protected int size()
  {
    return buffer.size();
  }

  private int firstCachedBatch()
  {
    return batchCount - buffer.size();
  }

  public void unloadBatch(int batchToUnload)
  {
    Preconditions.checkState(batchToUnload == firstCachedBatch());
    buffer.remove(0);
  }

  public Object batch(int batchIndex)
  {
    if (batchIndex >= batchCount) {
      return null;
    }
    int bufferIndex = batchIndex - (batchCount - buffer.size());
    return buffer.get(bufferIndex);
  }
}
