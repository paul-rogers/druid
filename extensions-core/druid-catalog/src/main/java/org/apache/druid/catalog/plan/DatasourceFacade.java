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

package org.apache.druid.catalog.plan;

import org.apache.druid.catalog.specs.CatalogUtils;
import org.apache.druid.catalog.specs.ClusterKeySpec;
import org.apache.druid.catalog.specs.table.Constants;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.java.util.common.granularity.Granularity;

import java.util.Collections;
import java.util.List;

public class DatasourceFacade extends TableFacade
{
  public DatasourceFacade(ResolvedTable resolved)
  {
    super(resolved);
  }

  public boolean isRollup()
  {
    return Constants.ROLLUP_DATASOURCE_TYPE.equals(spec().type());
  }

  public boolean isDetail()
  {
    return !isRollup();
  }

  public Granularity segmentGranularity()
  {
    String value = stringProperty(Constants.SEGMENT_GRANULARITY_FIELD);
    return value == null ? null : CatalogUtils.asDruidGranularity(value);
  }

  public Integer targetSegmentRows()
  {
    return intProperty(Constants.TARGET_SEGMENT_ROWS_FIELD);
  }

  @SuppressWarnings("unchecked")
  public List<ClusterKeySpec> clusterKeys()
  {
    return (List<ClusterKeySpec>) property(Constants.CLUSTER_KEYS_FIELD);
  }

  @SuppressWarnings("unchecked")
  public List<String> hiddenColumns()
  {
    Object value = property(Constants.HIDDEN_COLUMNS_FIELD);
    return value == null ? Collections.emptyList() : (List<String>) value;
  }

  public Granularity rollupGranularity()
  {
    String value = stringProperty(Constants.ROLLUP_GRANULARITY_FIELD);
    return value == null ? null : CatalogUtils.asDruidGranularity(value);
  }
}
