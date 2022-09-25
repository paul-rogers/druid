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

package org.apache.druid.catalog.specs;

import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;

import java.util.HashMap;
import java.util.Map;

public class Constants
{
  public static final String DESCRIPTION_FIELD = "description";
  public static final String SEGMENT_GRANULARITY_FIELD = "segmentGranularity";
  public static final String TARGET_SEGMENT_ROWS_FIELD = "targetSegmentRows";
  public static final String CLUSTER_KEYS_FIELD = "clusterKeys";
  public static final String HIDDEN_COLUMNS_FIELD = "hiddenColumns";
  public static final String ROLLUP_GRANULARITY_FIELD = "rollupGranularity";

  public static final String DETAIL_DATASOURCE_TYPE = "detail";
  public static final String ROLLUP_DATASOURCE_TYPE = "rollup";

  public static final String DETAIL_COLUMN_TYPE = "detail";
  public static final String DIMENSION_TYPE = "dimension";
  public static final String MEASURE_TYPE = "measure";
  public static final String INPUT_COLUMN_TYPE = "input";

  // Amazing that a parser doesn't already exist...
  private static final Map<String, Granularity> GRANULARITIES = new HashMap<>();

  static {
    GRANULARITIES.put("millisecond", Granularities.SECOND);
    GRANULARITIES.put("second", Granularities.SECOND);
    GRANULARITIES.put("minute", Granularities.MINUTE);
    GRANULARITIES.put("5 minute", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("5 minutes", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("five_minute", Granularities.FIVE_MINUTE);
    GRANULARITIES.put("10 minute", Granularities.TEN_MINUTE);
    GRANULARITIES.put("10 minutes", Granularities.TEN_MINUTE);
    GRANULARITIES.put("ten_minute", Granularities.TEN_MINUTE);
    GRANULARITIES.put("15 minute", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("15 minutes", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("fifteen_minute", Granularities.FIFTEEN_MINUTE);
    GRANULARITIES.put("30 minute", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("30 minutes", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("thirty_minute", Granularities.THIRTY_MINUTE);
    GRANULARITIES.put("hour", Granularities.HOUR);
    GRANULARITIES.put("6 hour", Granularities.SIX_HOUR);
    GRANULARITIES.put("6 hours", Granularities.SIX_HOUR);
    GRANULARITIES.put("six_hour", Granularities.SIX_HOUR);
    GRANULARITIES.put("day", Granularities.DAY);
    GRANULARITIES.put("week", Granularities.WEEK);
    GRANULARITIES.put("month", Granularities.MONTH);
    GRANULARITIES.put("quarter", Granularities.QUARTER);
    GRANULARITIES.put("year", Granularities.YEAR);
    GRANULARITIES.put("all", Granularities.ALL);
  }
}
