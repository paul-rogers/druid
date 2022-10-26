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
import org.apache.druid.exec.operator.ResultIterator.EofException;

import java.util.Iterator;
import java.util.List;

/**
 * Utility functions related to operators.
 */
public class Operators
{
  /**
   * Convenience function to open the operator and return its
   * iterator as an {@code Iterable}.
   */
  public static <T> Iterable<T> toIterable(Operator<T> op)
  {
    return new Iterable<T>() {
      @Override
      public Iterator<T> iterator()
      {
        return new Iterators.ShimIterator<T>(op.open());
      }
    };
  }

  public static <T> Iterator<T> toIterator(Operator<T> op)
  {
    return new Iterators.ShimIterator<T>(op.open());
  }

  /**
   * This will materialize the entire sequence from the wrapped
   * operator.  Use at your own risk.
   */
  public static <T> List<T> toList(Operator<T> op)
  {
    List<T> results = Lists.newArrayList(toIterator(op));
    op.close(true);
    return results;
  }

  public static EofException eof()
  {
    return new EofException();
  }
}
