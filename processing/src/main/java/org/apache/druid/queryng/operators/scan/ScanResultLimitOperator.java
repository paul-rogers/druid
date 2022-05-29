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

package org.apache.druid.queryng.operators.scan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.general.LimitOperator;

import java.util.ArrayList;
import java.util.List;

/**
 * This operator iterates over a ScanResultValue operator.  Its behaviour
 * varies depending on whether the query is returning time-ordered values and whether the CTX_KEY_OUTERMOST
 * flag is false.
 * <p>
 * Behaviours:
 * <ol>
 * <li>No time ordering: expects the child to produce ScanResultValues which each contain up to query.batchSize events.
 *     The operator will be "done" when the limit of events is reached.  The final ScanResultValue might contain
 *     fewer than batchSize events so that the limit number of events is returned.</li>
 * <li>Time Ordering, CTX_KEY_OUTERMOST false: Same behaviour as no time ordering.</li>
 * <li>Time Ordering, CTX_KEY_OUTERMOST=true or null: The child operator in this case should produce ScanResultValues
 *    that contain only one event each for the CachingClusteredClient n-way merge.  This operator will perform
 *    batching according to query batch size until the limit is reached.</li>
 * </ol>
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryLimitRowIterator}
 */
public class ScanResultLimitOperator extends LimitOperator
{
  private final boolean grouped;
  private final int batchSize;

  @VisibleForTesting
  public ScanResultLimitOperator(long limit, boolean grouped, int batchSize, Operator child)
  {
    super(limit, child);
    this.grouped = grouped;
    this.batchSize = batchSize;
  }

  @Override
  public Object next()
  {
    batchCount++;
    if (grouped) {
      return groupedNext();
    } else {
      return ungroupedNext();
    }
  }

  private ScanResultValue groupedNext()
  {
    ScanResultValue batch = (ScanResultValue) inputIter.next();
    List<?> events = (List<?>) batch.getEvents();
    if (events.size() <= limit - rowCount) {
      rowCount += events.size();
      return batch;
    } else {
      // last batch
      // single batch length is <= rowCount.MAX_VALUE, so this should not overflow
      int numLeft = (int) (limit - rowCount);
      rowCount = limit;
      return new ScanResultValue(batch.getSegmentId(), batch.getColumns(), events.subList(0, numLeft));
    }
  }

  private Object ungroupedNext()
  {
    // Perform single-event ScanResultValue batching at the outer level.  Each scan result value from the yielder
    // in this case will only have one event so there's no need to iterate through events.
    List<Object> eventsToAdd = new ArrayList<>(batchSize);
    List<String> columns = new ArrayList<>();
    while (eventsToAdd.size() < batchSize && inputIter.hasNext() && rowCount < limit) {
      ScanResultValue srv = (ScanResultValue) inputIter.next();
      // Only replace once using the columns from the first event
      columns = columns.isEmpty() ? srv.getColumns() : columns;
      eventsToAdd.add(Iterables.getOnlyElement((List<?>) srv.getEvents()));
      rowCount++;
    }
    return new ScanResultValue(null, columns, eventsToAdd);
  }
}
