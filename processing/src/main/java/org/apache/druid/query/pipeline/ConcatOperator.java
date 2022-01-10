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
import java.util.List;

import org.apache.druid.query.pipeline.Operator.IterableOperator;

import com.google.common.base.Preconditions;

/**
 * Concatenate a series of inputs. Simply returns the input values,
 * does not limit or coalesce batches. Starts child operators as late as
 * possible, and closes them as early as possible.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#mergeRunners}
 */
public class ConcatOperator implements IterableOperator
{
  public static Operator concatOrNot(List<Operator> children) {
    if (children.size() > 1) {
      return new ConcatOperator(children);
    }
    return children.get(0);
  }

  private final Iterator<Operator> childIter;
  private FragmentContext context;
  private Operator current;
  private Iterator<Object> currentIter;

  public ConcatOperator(List<Operator> children) {
    childIter = children.iterator();
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    this.context = context;
    return this;
  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (current != null) {
        if (currentIter.hasNext()) {
          return true;
        }
        current.close(true);
        current = null;
        currentIter = null;
      }
      if (!childIter.hasNext()) {
        return false;
      }
      current = childIter.next();
      currentIter = current.open(context);
    }
  }

  @Override
  public Object next() {
    Preconditions.checkState(currentIter != null, "Missing call to hasNext()?");
    return currentIter.next();
  }

  @Override
  public void close(boolean cascade) {
    if (cascade && current != null) {
      current.close(cascade);
    }
    current = null;
    currentIter = null;
  }
}
