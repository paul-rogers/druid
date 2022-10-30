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

package org.apache.druid.exec.shim;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchReader.BatchCursor;
import org.apache.druid.exec.batch.impl.AbstractBatchWriter;

import java.util.ArrayList;
import java.util.List;

/**
 * Base class for writers of batches represented by {@link List}.
 */
public abstract class ListWriter<T> extends AbstractBatchWriter<List<T>>
{
  protected class CopierImpl implements Copier
  {
    private final ListReader<T> source;

    public CopierImpl(ListReader<T> source)
    {
      this.source = source;
    }

    @Override
    public int copy(int n)
    {
      final BatchCursor sourceCursor = source.batchCursor();

      // The equivalent of advancing to the next row before reading.
      final int start = sourceCursor.index() + 1;
      final int availableCount = sourceCursor.size() - start;
      final int targetCount = Math.min(n, availableCount);
      final ArrayList<T> data = (ArrayList<T>) batch;
      final int capacity = sizeLimit - data.size();
      final int copyCount = Math.min(targetCount, capacity);
      if (copyCount <= 0) {
        return 0;
      }
      data.ensureCapacity(data.size() + copyCount);
      final List<T> sourceRows = source.rows();
      final int end = start + copyCount;
      for (int i = start; i < end; i++) {
        data.add(sourceRows.get(i));
      }

      // Position at EOF the copy stopped because of source availability.
      // This mimics next() returning false at EOF.
      int finalPosn = end - 1;
      if (copyCount == availableCount && copyCount < n && copyCount <= capacity) {
        finalPosn++;
      }
      sourceCursor.seek(finalPosn);
      return copyCount;
    }
  }

  protected List<T> batch;

  public ListWriter(final BatchSchema batchFactory)
  {
    super(batchFactory);
  }

  public ListWriter(final BatchSchema batchFactory, int sizeLimit)
  {
    super(batchFactory, sizeLimit);
  }

  @Override
  public void newBatch()
  {
    batch = new ArrayList<>();
  }

  @Override
  public List<T> harvest()
  {
    List<T> result = batch;
    batch = null;
    return result;
  }

  @Override
  public int size()
  {
    return batch == null ? 0 : batch.size();
  }
}
