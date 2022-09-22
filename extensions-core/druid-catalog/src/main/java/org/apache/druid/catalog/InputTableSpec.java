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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.catalog.TableMetadata.TableType;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ColumnSchema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Definition of an external input source, primarily for ingestion.
 * The components are derived from those for Druid ingestion: an
 * input source, a format and a set of columns. Also provides
 * properties, as do all table definitions.
 */
@UnstableApi
public class InputTableSpec extends TableSpec
{
  public static final String JSON_TYPE = "input";

  private final Map<String, Object> properties;
  private final List<InputColumnSpec> columns;

  public InputTableSpec(
      @JsonProperty("properties") final Map<String, Object> properties,
      @JsonProperty("columns") final List<InputColumnSpec> columns,
      @JsonProperty("tags") final Map<String, Object> tags
  )
  {
    super(tags);
    this.properties = properties;
    this.columns = columns;
  }

  @Override
  public TableType type()
  {
    return TableType.INPUT;
  }

  @JsonProperty("properties")
  public Map<String, Object> properties()
  {
    return properties;
  }

  @JsonProperty("columns")
  public List<InputColumnSpec> columns()
  {
    return columns;
  }

  @Override
  public void validate()
  {
    super.validate();
    if (properties == null || properties.isEmpty()) {
      throw new IAE("Input table properties are required");
    }
    if (columns == null || columns.isEmpty()) {
      throw new IAE("An input source must specify one or more columns");
    }
    Set<String> names = new HashSet<>();
    for (ColumnSpec col : columns) {
      if (!names.add(col.name())) {
        throw new IAE("Duplicate column name: " + col.name());
      }
      col.validate();
    }
  }

  @Override
  public String defaultSchema()
  {
    return TableId.INPUT_SCHEMA;
  }

  public RowSignature rowSignature()
  {
    RowSignature.Builder builder = RowSignature.builder();
    if (columns() != null) {
      for (InputColumnSpec col : columns()) {
        ColumnType druidType = Columns.SQL_TO_DRUID_TYPES.get(StringUtils.toUpperCase(col.sqlType()));
        if (druidType == null) {
          druidType = ColumnType.STRING;
        }
        builder.add(col.name(), druidType);
      }
    }
    return builder.build();
  }

  public List<ColumnSchema> columnSchemas()
  {
    if (columns == null) {
      return Collections.emptyList();
    }
    return columns.stream().map(col -> col.toSchema()).collect(Collectors.toList());
  }

  public static Builder builder()
  {
    return new Builder();
  }

  public Builder toBuilder()
  {
    return new Builder(this);
  }

  /**
   * Merge an input spec.
   * <ul>
   * <li>Given tags replace existing ones. Null values indicate deletions.</li>
   * <li>Properties are merged the same way.</li>
   * <li>Columns, if provided, <i>replace</i> existing columns since column
   * order is critical for input sources.</li>
   * </ul>
   */
  @Override
  public TableSpec merge(TableSpec update, Map<String, Object> raw, ObjectMapper mapper)
  {
    if (!(update instanceof InputTableSpec)) {
      throw new IAE("The update must be of type [%s]", JSON_TYPE);
    }
    InputTableSpec inputUpdate = (InputTableSpec) update;
    Builder builder = toBuilder();
    builder.tags(CatalogUtils.mergeMap(tags(), update.tags()));
    builder.properties(CatalogUtils.mergeMap(properties, inputUpdate.properties));
    if (raw.containsKey("columns")) {
      builder.columns(inputUpdate.columns);
    }
    return builder.build();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    InputTableSpec other = (InputTableSpec) o;
    return Objects.equals(this.properties, other.properties)
        && Objects.equals(this.columns, other.columns)
        && Objects.equals(this.tags(), other.tags());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        properties,
        columns,
        tags());
  }

  public static class Builder
  {
    private Map<String, Object> properties;
    private List<InputColumnSpec> columns;
    private Map<String, Object> tags;

    public Builder()
    {
      this.properties = new HashMap<>();
      this.columns = new ArrayList<>();
    }

    public Builder(InputTableSpec spec)
    {
      this.properties = new HashMap<>(spec.properties);
      this.columns = new ArrayList<>(spec.columns);
      if (spec.tags() == null) {
        this.tags = null;
      } else {
        this.tags = new HashMap<>(spec.tags());
      }
    }

    public Builder properties(Map<String, Object> properties)
    {
      this.properties = properties;
      return this;
    }

    public Builder property(String key, Object value)
    {
      properties.put(key, value);
      return this;
    }

    public Map<String, Object> properties()
    {
      return properties;
    }

    public List<InputColumnSpec> columns()
    {
      return columns;
    }

    public Builder columns(List<InputColumnSpec> columns)
    {
      this.columns = columns;
      return this;
    }

    public Builder column(InputColumnSpec column)
    {
      if (Strings.isNullOrEmpty(column.name())) {
        throw new IAE("Column name is required");
      }
      columns.add(column);
      return this;
    }

    public Builder column(String name, String sqlType)
    {
      return column(new InputColumnSpec(name, sqlType));
    }

    public Builder tags(Map<String, Object> properties)
    {
      this.tags = properties;
      return this;
    }

    public Builder tag(String key, Object value)
    {
      if (tags == null) {
        tags = new HashMap<>();
      }
      tags.put(key, value);
      return this;
    }

    public Map<String, Object> tags()
    {
      return tags;
    }

    public InputTableSpec build()
    {
      return new InputTableSpec(
          properties,
          columns,
          tags
          );
    }
  }
}
