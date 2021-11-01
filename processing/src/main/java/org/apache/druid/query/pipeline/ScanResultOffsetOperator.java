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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.Operator.IterableOperator;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;

/**
 * Offset that skips a given number of rows on top of a skips ScanQuery. It is used to implement
 * the "offset" feature.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryOffsetSequence}
 */
public class ScanResultOffsetOperator implements IterableOperator
{
  public static ScanResultOffsetOperator forQuery(ScanQuery query, Operator child) {
    ScanQuery.ResultFormat resultFormat = query.getResultFormat();
    if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
      throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
    }
    long offset = query.getScanRowsOffset();
    if (offset < 1) {
      throw new IAE("'offset' must be greater than zero");
    }
    return new ScanResultOffsetOperator(offset, child);
  }

  private final Operator input;
  private final long offset;
  private Iterator<Object> inputIter;
  private long rowCount;
  @SuppressWarnings("unused")
  private int batchCount;
  private ScanResultValue lookAhead;
  private boolean done;

  public ScanResultOffsetOperator(long offset, Operator input)
  {
    this.offset = offset;
    this.input = input;
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    inputIter = input.open(context);
    return this;
  }

  @Override
  public boolean hasNext()
  {
    if (done) {
      return false;
    }
    if (rowCount == 0) {
      return skip();
    }
    done = !inputIter.hasNext();
    return !done;
  }

  @Override
  public Object next()
  {
    if (lookAhead != null) {
      ScanResultValue result = lookAhead;
      lookAhead = null;
      return result;
    }
    return inputIter.next();
  }

  private boolean skip()
  {
    while (true) {
      if (!inputIter.hasNext()) {
        done = true;
        return false;
      }
      ScanResultValue batch = (ScanResultValue) inputIter.next();
      final List<?> rows = (List<?>) batch.getEvents();
      final int eventCount = rows.size();
      final long toSkip = offset - rowCount;
      if (toSkip >= eventCount) {
        rowCount += eventCount;
        continue;
      }
      rowCount += eventCount - toSkip;
      lookAhead = new ScanResultValue(
          batch.getSegmentId(),
          batch.getColumns(),
          rows.subList((int) toSkip, eventCount));
      return true;
    }
  }

//  private boolean ungroupedSkip() {
//    while (rowCount < defn.offset) {
//      if (!input.hasNext()) {
//        done = true;
//        return false;
//      }
//      input.next();
//      rowCount++;
//    }
//    return input.hasNext();
//  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      input.close(cascade);
    }
  }
}
