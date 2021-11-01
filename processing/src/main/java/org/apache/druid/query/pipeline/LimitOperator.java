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

package org.apache.druid.query.pipeline;

import java.util.Iterator;

import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.Operator.IterableOperator;

public abstract class LimitOperator implements IterableOperator
{
  public static final long UNLIMITED = Long.MAX_VALUE;

  protected final long limit;
  protected final Operator input;
  protected Iterator<Object> inputIter;
  protected long rowCount;
  protected int batchCount;

  public LimitOperator(long limit, Operator input)
  {
    this.limit = limit;
    this.input = input;
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    inputIter = input.open(context);
    return this;
  }

  @Override
  public boolean hasNext() {
    return rowCount < limit && inputIter.hasNext();
  }

  @Override
  public Object next()
  {
    rowCount++;
    return inputIter.next();
  }

  @Override
  public void close(boolean cascade)
  {
    inputIter = null;
    if (cascade) {
      input.close(cascade);
    }
  }
}
