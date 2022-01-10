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

/**
 * Operator which relabels its thread during its execution.
 *
 * @see {@link org.apache.druid.query.spec.SpecificSegmentQueryRunner}
 */
public class ThreadLabelOperator implements Operator
{
  private final String label;
  private final Operator child;
  private String originalLabel;

  public ThreadLabelOperator(final String label, final Operator child)
  {
    this.label = label;
    this.child = child;
  }

 @Override
  public Iterator<Object> open(FragmentContext context) {
    final Thread currThread = Thread.currentThread();
    originalLabel = currThread.getName();
    currThread.setName(label);
    return child.open(context);
  }

  @Override
  public void close(boolean cascade) {
    try {
      if (cascade) {
        child.close(cascade);
      }
    } finally {
      if (originalLabel != null) {
        Thread.currentThread().setName(originalLabel);
        originalLabel = null;
      }
    }
  }
}
