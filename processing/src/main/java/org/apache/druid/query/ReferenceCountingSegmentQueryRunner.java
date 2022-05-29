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

package org.apache.druid.query;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.planner.QueryPlanner;
import org.apache.druid.segment.SegmentReference;

public class ReferenceCountingSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunnerFactory<T, Query<T>> factory;
  private final SegmentReference segment;
  private final SegmentDescriptor descriptor;

  public ReferenceCountingSegmentQueryRunner(
      QueryRunnerFactory<T, Query<T>> factory,
      SegmentReference segment,
      SegmentDescriptor descriptor
  )
  {
    this.factory = factory;
    this.segment = segment;
    this.descriptor = descriptor;
  }

  @Override
  public Sequence<T> run(final QueryPlus<T> queryPlus, ResponseContext responseContext)
  {
    if (Operators.enabledFor(queryPlus)) {
      return QueryPlanner.runSeqmentLock(segment, descriptor, queryPlus, factory, responseContext);
    }
    return segment.acquireReferences().map(closeable -> {
      try {
        final Sequence<T> baseSequence = factory.createRunner(segment).run(queryPlus, responseContext);
        return Sequences.withBaggage(baseSequence, closeable);
      }
      catch (Throwable t) {
        try {
          closeable.close();
        }
        catch (Exception e) {
          t.addSuppressed(e);
        }
        throw t;
      }
    }).orElseGet(() -> new ReportTimelineMissingSegmentQueryRunner<T>(descriptor).run(queryPlus, responseContext));
  }
}
