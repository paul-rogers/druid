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

package org.apache.druid.exec.operator;

import com.google.common.collect.Lists;
import org.apache.druid.exec.internalSort.Operators;
import org.apache.druid.exec.operator.ResultIterator.EofException;
import org.apache.druid.exec.operator.ResultIterator.StallException;
import org.apache.druid.java.util.common.UOE;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Utility methods on top of {@link ResultIterator RowIterator},
 * including conversion to a Java iterator (primarily for testing.)
 */
public class Iterators
{
  public static class ShimIterator implements Iterator<Batch>
  {
    private final ResultIterator operIter;
    private boolean eof;
    private Batch lookAhead;

    public ShimIterator(ResultIterator operIter)
    {
      this.operIter = operIter;
    }

    @Override
    public boolean hasNext()
    {
      if (eof) {
        return false;
      }
      try {
        lookAhead = operIter.next();
        return true;
      }
      catch (EofException e) {
        eof = true;
        return false;
      }
      catch (StallException e) {
        throw new UOE("Async mode not supported for a wrapped iterator.");
      }
    }

    @Override
    public Batch next()
    {
      if (eof || lookAhead == null) {
        throw new NoSuchElementException();
      }
      return lookAhead;
    }
  }

  public static Iterable<Batch> toIterable(ResultIterator iter)
  {
    return Iterators.toIterable(Iterators.toIterator(iter));
  }

  public static <T> Iterable<T> toIterable(Iterator<T> iter)
  {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator()
      {
        return iter;
      }
    };
  }

  public static Iterator<Batch> toIterator(ResultIterator opIter)
  {
    return new ShimIterator(opIter);
  }

  public static List<Batch> toList(ResultIterator operIter)
  {
    return Lists.newArrayList(new ShimIterator(operIter));
  }

  public static ResultIterator emptyIterator()
  {
    return new ResultIterator()
    {
      @Override
      public Batch next() throws EofException
      {
        throw Operators.eof();
      }
    };
  }

  public static ResultIterator singletonIterator(Batch item)
  {
    return new ResultIterator()
    {
      private boolean eof;

      @Override
      public Batch next() throws EofException
      {
        if (eof) {
          throw Operators.eof();
        }
        eof = true;
        return item;
      }
    };
  }
}
