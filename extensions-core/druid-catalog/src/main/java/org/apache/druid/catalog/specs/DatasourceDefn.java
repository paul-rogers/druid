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

import com.fasterxml.jackson.core.type.TypeReference;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class DatasourceDefn extends TableDefn
{
  public static class DetailDatasourceDefn extends DatasourceDefn
  {
    public DetailDatasourceDefn()
    {
      super(
          "Detail datasource",
          Constants.DETAIL_DATASOURCE_TYPE,
          CatalogObjectDefn.toFieldMap(
              TableDefn.genericFields,
              datasourceFields
          ),
          TableDefn.toColumnMap(
              Collections.singletonList(new ColumnDefn.DetailColumnDefn())
          )
      );
    }
  }

  public static class RollupDatasourceDefn extends DatasourceDefn
  {
    static final CatalogFieldDefn<?>[] rollupDatasourceFields = {
        new CatalogFieldDefn.GranularityFieldDefn(Constants.ROLLUP_GRANULARITY_FIELD),
    };

    public RollupDatasourceDefn()
    {
      super(
          "Rollup datasource",
          Constants.ROLLUP_DATASOURCE_TYPE,
          CatalogObjectDefn.toFieldMap(
              TableDefn.genericFields,
              datasourceFields,
              rollupDatasourceFields
          ),
          TableDefn.toColumnMap(
              Arrays.asList(
                  new ColumnDefn.DimensionDefn(),
                  new ColumnDefn.MeasureDefn()
              )
          )
      );
    }
  }

  protected static final CatalogFieldDefn<?>[] datasourceFields = {
      new CatalogFieldDefn.SegmentGranularityFieldDefn(),
      new CatalogFieldDefn.IntFieldDefn(Constants.TARGET_SEGMENT_ROWS_FIELD),
      new CatalogFieldDefn.ListFieldDefn<ClusterKeySpec>(Constants.CLUSTER_KEYS_FIELD, FieldTypes.CLUSTER_KEY_LIST_TYPE),
      new CatalogFieldDefn.HiddenColumnsDefn()
  };

  public DatasourceDefn(
      final String name,
      final String typeValue,
      final Map<String, CatalogFieldDefn<?>> fields,
      final Map<String, ColumnDefn> columnTypes
  )
  {
    super(name, typeValue, fields, columnTypes);
  }
}