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

package org.apache.druid.queryng.operators.general;

import com.google.common.collect.Lists;
import org.apache.druid.query.BySegmentResultValueClass;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

/**
 * Operator that consumes a base single-segment query operator, and wraps its results in a
 * {@link BySegmentResultValueClass} object. Added to the DAG if the "bySegment" query context
 * parameter is set.
 * <p>
 * Note that this operator may change the type of query from the T of the query. The results
 * may really be of type {@code Result<BySegmentResultValue<T>>}, if "bySegment" is set. Downstream consumers
 * of the returned sequence must be aware of this, and can use {@link QueryContexts#isBySegment(Query)} to
 * know what to expect.
 * <p>
 * The implementation mimics the original query runner (see below) which materializes the entire
 * input into a single list. Such a design is clearly not ideal for large result sets. Can the
 * design be modified to allow multiple batches of rows within a single segment?
 *
 * @see {@link org.apache.druid.query.BySegmentQueryRunner}
 */
public class BySegmentOperator implements Operator
{
  private final String segmentId;
  private final DateTime timestamp;
  private final Interval interval;
  private final Supplier<Operator> inputSupplier;

  public BySegmentOperator(
      final String segmentId,
      final DateTime timestamp,
      final Interval interval,
      final Supplier<Operator> inputSupplier
  )
  {
    this.segmentId = segmentId;
    this.timestamp = timestamp;
    this.interval = interval;
    this.inputSupplier = inputSupplier;
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    // Read the entire input result set into a list
    Operator child = inputSupplier.get();
    List<Object> results = Lists.newArrayList(Operators.toIterable(child, context));

    // The child is now done, close it.
    child.close(true);

    // If no results, return an empty result set.
    // TODO: Seems reasonable, but is different than original code.
    if (results.isEmpty()) {
      return Collections.emptyIterator();
    }

    // Put into the result object then return an iterator over a list
    // with just that object. Note the intermediate Object result keeps
    // the type system happy.
    Object result = new Result<>(
        timestamp,
        new BySegmentResultValueClass<>(
            results,
            segmentId,
            interval
        )
    );
    return Collections.singletonList(result).iterator();
  }

  @Override
  public void close(boolean cascade)
  {
  }
}
