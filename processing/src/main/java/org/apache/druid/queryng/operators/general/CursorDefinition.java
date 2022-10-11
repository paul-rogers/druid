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

import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.queryng.operators.SequenceIterator;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.SegmentMissingException;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.vector.VectorCursor;
import org.joda.time.Interval;

import javax.annotation.Nullable;

public class CursorDefinition
{
  public final Segment segment;
  public final Interval queryInterval;
  @Nullable public final Filter filter;
  @Nullable public final VirtualColumns virtualColumns;
  public final boolean descending;
  public final Granularity granularity;
  @Nullable public final QueryMetrics<?> queryMetrics;
  private final int vectorSize;
  StorageAdapter adapter;

  public CursorDefinition(
      final Segment segment,
      final Interval queryInterval,
      @Nullable final Filter filter,
      @Nullable final VirtualColumns virtualColumns,
      final boolean descending,
      final Granularity granularity,
      @Nullable final QueryMetrics<?> queryMetrics,
      final int vectorSize
  )
  {
    this.segment = segment;
    this.queryInterval = queryInterval;
    this.filter = filter;
    this.virtualColumns = virtualColumns;
    this.descending = descending;
    this.granularity = granularity;
    this.queryMetrics = queryMetrics;
    this.vectorSize = vectorSize;
  }

  public void setAdapter(StorageAdapter adapter)
  {
    this.adapter = adapter;
    if (adapter == null) {
      throw new SegmentMissingException(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }
  }

  public StorageAdapter adapter()
  {
    if (adapter == null) {
      setAdapter(segment.asStorageAdapter());
    }
    return adapter;
  }

  public SequenceIterator<Cursor> cursors()
  {
    return SequenceIterator.of(
        adapter().makeCursors(
            filter,
            queryInterval,
            virtualColumns,
            granularity,
            descending,
            queryMetrics
        )
    );
  }

  public VectorCursor vectorCursor()
  {
    return adapter().makeVectorCursor(
        filter,
        queryInterval,
        virtualColumns,
        descending,
        vectorSize,
        queryMetrics
    );
  }
}
