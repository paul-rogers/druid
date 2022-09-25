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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.Columns;
import org.apache.druid.catalog.MeasureTypes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.Collections;
import java.util.Map;

public class ColumnDefn extends CatalogObjectDefn
{
  public static class DetailColumnDefn extends ColumnDefn
  {
    public DetailColumnDefn()
    {
      super(
          "Column",
          Constants.DETAIL_COLUMN_TYPE,
          Collections.emptyMap()
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      validateScalarColumn(spec);
    }
  }

  public static class DimensionDefn extends ColumnDefn
  {
    public DimensionDefn()
    {
      super(
          "Dimension",
          Constants.DIMENSION_TYPE,
          Collections.emptyMap()
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      validateScalarColumn(spec);
    }
  }

  public static class MeasureDefn extends ColumnDefn
  {
    public MeasureDefn()
    {
      super(
          "Measure",
          Constants.MEASURE_TYPE,
          Collections.emptyMap()
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      if (spec.sqlType() == null) {
        throw new IAE("A type is required for measure column " + spec.name());
      }
      if (Columns.isTimeColumn(spec.name())) {
        throw new IAE(StringUtils.format(
            "%s column cannot be a measure",
            Columns.TIME_COLUMN
            ));
      }
      MeasureTypes.parse(spec.sqlType());
    }
  }

  /**
   * Convenience class that holds a column specification and its corresponding
   * definition. This allows the spec to be a pure "data object" without knowledge
   * of the metadata representation given by the column definition.
   */
  public static class ResolvedColumn
  {
    private final ColumnDefn defn;
    private final ColumnSpec spec;

    public ResolvedColumn(ColumnDefn defn, ColumnSpec spec)
    {
      this.defn = defn;
      this.spec = spec;
    }

    public ColumnDefn defn()
    {
      return defn;
    }

    public ColumnSpec spec()
    {
      return spec;
    }

    public ResolvedColumn merge(ColumnSpec update)
    {
      return new ResolvedColumn(defn, defn.merge(spec, update));
    }

    public void validate(ObjectMapper jsonMapper)
    {
      defn.validate(spec, jsonMapper);
    }
  }

  public ColumnDefn(
      final String name,
      final String typeValue,
      final Map<String, CatalogFieldDefn<?>> fields
  )
  {
    super(name, typeValue, fields);
  }

  public ColumnSpec merge(ColumnSpec spec, ColumnSpec update)
  {
    String updateType = update.type();
    if (updateType != null && !spec.type().equals(updateType)) {
      throw new IAE("The update type must be null or [%s]", spec.type());
    }
    String revisedType = update.sqlType() == null ? spec.sqlType() : update.sqlType();
    Map<String, Object> revisedProps = mergeProperties(
        spec.properties(),
        update.properties()
    );
    return new ColumnSpec(spec.type(), spec.name(), revisedType, revisedProps);
  }

  public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
  {
    spec.validate();
  }

  public void validateScalarColumn(ColumnSpec spec)
  {
    Columns.validateScalarColumn(spec.name(), spec.sqlType());
    if (Columns.isTimeColumn(spec.name())) {
      if (spec.sqlType() != null && !Columns.TIMESTAMP.equalsIgnoreCase(spec.sqlType())) {
        throw new IAE(StringUtils.format(
            "%s column must have no SQL type or SQL type %s",
            Columns.TIME_COLUMN,
            Columns.TIMESTAMP
            ));
      }
    }
  }
}
