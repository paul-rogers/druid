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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.pipeline.Operator.IterableOperator;

import java.io.IOException;
import java.util.Iterator;

/**
 * The <code>SequenceOperator</code> wraps a {@link Sequence} in the
 * operator protocol. The operator will make (at most) one pass through
 * the sequence. The sequence's yielder will be defined in <code>start()</code>,
 * which may cause the sequence to start doing work and obtaining resources.
 * Each call to <code>next()</code>/<code>get()</code> will yield one result
 * from the sequence. The <code>close()</code> call will close the yielder
 * for the sequence, which should release any resources held by the sequence.
 *
 * @param <T>
 */
public class SequenceOperator implements IterableOperator
{
  private final Sequence<Object> sequence;
  private Yielder<Object> yielder;

  @SuppressWarnings("unchecked")
  public SequenceOperator(Sequence<?> sequence)
  {
    this.sequence = (Sequence<Object>) sequence;
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    Preconditions.checkState(yielder == null);
    yielder = sequence.toYielder(
        null,
        new YieldingAccumulator<Object, Object>()
        {
           @Override
          public Object accumulate(Object accumulated, Object in)
          {
            yield();
            return in;
          }
        }
    );
    return this;
  }

  @Override
  public boolean hasNext()
  {
    return yielder != null && !yielder.isDone();
  }

  @Override
  public Object next()
  {
    Preconditions.checkState(yielder != null);
    Object value = yielder.get();
    yielder = yielder.next(null);
    return value;
  }

  @Override
  public void close(boolean cascade)
  {
    if (yielder == null) {
      return;
    }
    try {
      yielder.close();
    } catch (IOException e) {
      throw new RuntimeException(e.getMessage(), e);
    }
    finally {
      yielder = null;
    }
  }
}
