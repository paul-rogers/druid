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

package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.operator.BatchReader.BatchCursor;

/**
 * Cursor for a typical batch of data which allows both sequential and
 * random access to a fixed-sized batch.
 */
public class SeekableCursor implements BatchCursor
{
  public interface PositionListener
  {
    void updatePosition(int posn);
  }

  protected int size;
  protected PositionListener listener;
  protected int posn;

  public SeekableCursor()
  {
    this.listener = p -> { };
  }

  public void bind(int size)
  {
    this.size = size;
    reset();
  }

  public void bindListener(final PositionListener listener)
  {
    this.listener = listener;
  }

  @Override
  public void reset()
  {
    // If the batch is empty, start at EOF. Else, start
    // before the first row.
    this.posn = size == 0 ? 0 : -1;
  }

  @Override
  public boolean next()
  {
    if (++posn >= size) {
      posn = size();
      return false;
    }
    listener.updatePosition(posn);
    return true;
  }

  @Override
  public boolean seek(int newPosn)
  {
    // Bound the new position to the valid range. If the
    // batch is empty, the new position will be -1: before the
    // (non-existent) first row.
    if (newPosn < 0) {
      reset();
      return false;
    } else if (newPosn >= size()) {
      posn = size();
      return false;
    }
    posn = newPosn;
    listener.updatePosition(posn);
    return true;
  }

  @Override
  public int index()
  {
    return posn;
  }

  @Override
  public int size()
  {
    return size;
  }

  @Override
  public boolean isEOF()
  {
    return posn == size;
  }
}
