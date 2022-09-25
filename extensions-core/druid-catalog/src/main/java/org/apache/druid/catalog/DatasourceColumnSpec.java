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

package org.apache.druid.catalog;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.catalog.MeasureTypes.MeasureType;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;

import java.util.Map;

/**
 * Description of a detail datasource column and a rollup
 * dimension or measure column.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = DatasourceColumnSpec.DetailColumnSpec.JSON_TYPE, value = DatasourceColumnSpec.DetailColumnSpec.class),
    @Type(name = DatasourceColumnSpec.DimensionSpec.JSON_TYPE, value = DatasourceColumnSpec.DimensionSpec.class),
    @Type(name = DatasourceColumnSpec.MeasureSpec.JSON_TYPE, value = DatasourceColumnSpec.MeasureSpec.class),
})
@UnstableApi
public abstract class DatasourceColumnSpec extends ColumnSpec
{
  @JsonCreator
  public DatasourceColumnSpec(
      @JsonProperty("name") String name,
      @JsonProperty("sqlType") String sqlType,
      @JsonProperty("tags") Map<String, Object> tags
  )
  {
    super(name, sqlType, tags);
  }

  public abstract ColumnType druidType();
  public abstract DatasourceColumnSpec merge(DatasourceColumnSpec update);

  public static class DetailColumnSpec extends DatasourceColumnSpec
  {
    public static final String JSON_TYPE = "column";

    @JsonCreator
    public DetailColumnSpec(
        @JsonProperty("name") String name,
        @JsonProperty("sqlType") String sqlType,
        @JsonProperty("tags") Map<String, Object> tags
    )
    {
      super(name, sqlType, tags);
    }

    @Override
    protected ColumnKind kind()
    {
      return ColumnKind.DETAIL;
    }

    @Override
    public ColumnType druidType()
    {
      if (Columns.isTimeColumn(name)) {
        return ColumnType.LONG;
      } else if (sqlType == null) {
        return null;
      } else {
        return Columns.druidType(sqlType);
      }
    }

    @Override
    public void validate()
    {
      super.validate();
      Columns.validateScalarColumn(name, sqlType);
      if (Columns.isTimeColumn(name)) {
        if (sqlType != null && !Columns.TIMESTAMP.equalsIgnoreCase(sqlType)) {
          throw new IAE(StringUtils.format(
              "%s column must have no SQL type or SQL type %s",
              Columns.TIME_COLUMN,
              Columns.TIMESTAMP
              ));
        }
      }
    }

    @Override
    public DatasourceColumnSpec merge(DatasourceColumnSpec update)
    {
      if (!(update instanceof DetailColumnSpec)) {
        throw new IAE("The update must be of type [%s]", JSON_TYPE);
      }
      DetailColumnSpec col = (DetailColumnSpec) update;
      return new DetailColumnSpec(
          name,
          col.sqlType == null ? sqlType : col.sqlType(),
          CatalogUtils.mergeMap(tags, col.tags())
      );
    }
  }

  public abstract static class RollupColumnSpec  extends DatasourceColumnSpec
  {
    @JsonCreator
    public RollupColumnSpec(
        final String name,
        final String sqlType,
        final Map<String, Object> tags
    )
    {
      super(name, sqlType, tags);
    }
  }

  public static class DimensionSpec extends RollupColumnSpec
  {
    public static final String JSON_TYPE = "dimension";

    @JsonCreator
    public DimensionSpec(
        @JsonProperty("name") final String name,
        @JsonProperty("sqlType") final String sqlType,
        @JsonProperty("tags") final Map<String, Object> tags
    )
    {
      super(name, sqlType, tags);
    }

    @Override
    protected ColumnKind kind()
    {
      return ColumnKind.DIMENSION;
    }

    @Override
    public ColumnType druidType()
    {
      return Columns.druidType(sqlType);
    }

    @Override
    public void validate()
    {
      super.validate();
      Columns.validateScalarColumn(name, sqlType);
      if (Columns.isTimeColumn(name)) {
        if (sqlType != null && !Columns.TIMESTAMP.equalsIgnoreCase(sqlType)) {
          throw new IAE(StringUtils.format(
              "%s column must have no SQL type or SQL type %s",
              Columns.TIME_COLUMN,
              Columns.TIMESTAMP
              ));
        }
      }
    }

    @Override
    public DatasourceColumnSpec merge(DatasourceColumnSpec update)
    {
      if (!(update instanceof DimensionSpec)) {
        throw new IAE("The update must be of type [%s]", JSON_TYPE);
      }
      DimensionSpec col = (DimensionSpec) update;
      return new DimensionSpec(
          name,
          col.sqlType == null ? sqlType : col.sqlType(),
              CatalogUtils.mergeMap(tags, col.tags())
      );
    }
  }

  /**
   * Catalog definition of a measure (metric) column.
   * Types are expressed as compound types: "AGG_FN(ARG_TYPE,...)"
   * where "AGG_FN" is one of the supported aggregate functions,
   * and "ARG_TYPE" is zero or more argument types.
   */
  public static class MeasureSpec extends DatasourceColumnSpec
  {
    public static final String JSON_TYPE = "measure";

    @JsonCreator
    public MeasureSpec(
        @JsonProperty("name") String name,
        @JsonProperty("sqlType") String sqlType,
        @JsonProperty("tags") Map<String, Object> tags
    )
    {
      super(name, sqlType, tags);
    }

    @Override
    protected ColumnKind kind()
    {
      return ColumnKind.MEASURE;
    }

    @Override
    public void validate()
    {
      super.validate();
      if (sqlType == null) {
        throw new IAE("A type is required for measure column " + name);
      }
      if (Columns.isTimeColumn(name)) {
        throw new IAE(StringUtils.format(
            "%s column cannot be a measure",
            Columns.TIME_COLUMN
            ));
      }
      MeasureTypes.parse(sqlType);
    }

    public MeasureType measureType()
    {
      if (sqlType == null) {
        return null;
      }
      try {
        return MeasureTypes.parse(sqlType);
      }
      catch (ISE e) {
        return null;
      }
    }

    @Override
    public ColumnType druidType()
    {
      MeasureType typeRef = measureType();
      return typeRef == null ? null : typeRef.storageType;
    }

    @Override
    public DatasourceColumnSpec merge(DatasourceColumnSpec update)
    {
      if (!(update instanceof MeasureSpec)) {
        throw new IAE("The update must be of type [%s]", JSON_TYPE);
      }
      MeasureSpec col = (MeasureSpec) update;
      return new MeasureSpec(
          name,
          col.sqlType == null ? sqlType : col.sqlType(),
              CatalogUtils.mergeMap(tags, col.tags())
      );
    }
  }
}
