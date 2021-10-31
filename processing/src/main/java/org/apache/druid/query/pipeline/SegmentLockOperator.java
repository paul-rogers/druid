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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.Optional;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.segment.SegmentReference;

/**
 * @{see {@link org.apache.druid.query.ReferenceCountingSegmentQueryRunner}
 */
public class SegmentLockOperator implements Operator
{
  private static final Logger LOG = new Logger(SegmentLockOperator.class);

  private final SegmentReference segment;
  private final SegmentDescriptor descriptor;
  private final Operator child;
  private Closeable lock;

  public SegmentLockOperator(
      SegmentReference segment,
      SegmentDescriptor descriptor,
      Operator child
  )
  {
    this.segment = segment;
    this.descriptor = descriptor;
    this.child = child;
  }

  @Override
  public Iterator<Object> open(FragmentContext context) {
    Optional<Closeable> maybeLock = segment.acquireReferences();
    if (maybeLock.isPresent()) {
      lock = maybeLock.get();
      return child.open(context);
    } else {
      LOG.debug("Reporting a missing segment[%s] for query[%s]", descriptor, context.queryId());
      context.responseContext().add(ResponseContext.Key.MISSING_SEGMENTS, descriptor);
      return Collections.emptyIterator();
    }
  }

//  @Override
//  public boolean hasNext() {
//    return lock != null && child.hasNext();
//  }
//
//  @Override
//  public Object next() {
//    return child.next();
//  }

  @Override
  public void close(boolean cascade) {
    if (lock != null) {
      try {
        lock.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close segment " + descriptor.toString(), e);
      }
      lock = null;
    }
    if (cascade) {
      child.close(cascade);
    }
  }
}
