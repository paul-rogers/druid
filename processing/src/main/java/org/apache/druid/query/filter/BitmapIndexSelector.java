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

package org.apache.druid.query.filter;

import com.google.errorprone.annotations.MustBeClosed;
import org.apache.druid.collections.bitmap.BitmapFactory;
import org.apache.druid.collections.bitmap.ImmutableBitmap;
import org.apache.druid.collections.spatial.ImmutableRTree;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.CloseableIndexed;
import org.apache.druid.segment.filter.DimensionPredicateFilter;

import javax.annotation.Nullable;

/**
 */
public interface BitmapIndexSelector
{
  public interface BitmapMetrics
  {
    void bitmapIndex(String dimension, int cardinality, String value, ImmutableBitmap bitmap);
    void shortCircuit(boolean value);
    void predicateFilter(String dimension, BitmapIndexSelector selector, DimensionPredicateFilter filter, Object bitmap);
  }

  public static class BitmapMetricsStub implements BitmapMetrics
  {
    @Override
    public void bitmapIndex(String dimension, int cardinality, String value, ImmutableBitmap bitmap)
    {
    }

    @Override
    public void shortCircuit(boolean value)
    {
    }

    @Override
    public void predicateFilter(String dimension, BitmapIndexSelector selector, DimensionPredicateFilter filter, Object bitmap)
    {
    }
  }

  BitmapMetrics METRICS_STUB = new BitmapMetricsStub();

  @MustBeClosed
  @Nullable
  CloseableIndexed<String> getDimensionValues(String dimension);
  ColumnCapabilities.Capable hasMultipleValues(String dimension);
  int getNumRows();
  BitmapFactory getBitmapFactory();
  @Nullable
  BitmapIndex getBitmapIndex(String dimension);
  @Nullable
  ImmutableBitmap getBitmapIndex(String dimension, String value);
  ImmutableRTree getSpatialIndex(String dimension);
  BitmapMetrics getMetrics();
  int getCardinality(String dimension);
}
