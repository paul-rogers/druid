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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.IAE;

import java.util.Map;
import java.util.Objects;

/**
 * Base class for table columns. Columns have multiple types
 * represented as subclasses.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = "datasource", value = DatasourceSpec.class),
    @Type(name = "input", value = InputTableSpec.class),
    @Type(name = "tombstone", value = TableSpec.Tombstone.class),
})
@UnstableApi
public abstract class ColumnSpec
{
  enum ColumnKind
  {
    DETAIL,
    DIMENSION,
    MEASURE,
    INPUT
  }

  protected final String name;
  protected final String sqlType;
  protected final Map<String, Object> tags;

  public ColumnSpec(
      String name,
      String sqlType
  )
  {
    this(name, sqlType, null);
  }

  public ColumnSpec(
      String name,
      String sqlType,
      Map<String, Object> tags
  )
  {
    this.name = name;
    this.sqlType = sqlType;
    this.tags = tags == null || tags.size() == 0 ? null : tags;
  }

  protected abstract ColumnKind kind();

  @JsonProperty("name")
  public String name()
  {
    return name;
  }

  @JsonProperty("sqlType")
  @JsonInclude(Include.NON_NULL)
  public String sqlType()
  {
    return sqlType;
  }

  @JsonProperty("tags")
  @JsonInclude(Include.NON_EMPTY)
  public Map<String, Object> tags()
  {
    return tags;
  }

  public void validate()
  {
    if (Strings.isNullOrEmpty(name)) {
      throw new IAE("Column name is required");
    }
  }

  public byte[] toBytes(ObjectMapper jsonMapper)
  {
    return CatalogSpecs.toBytes(jsonMapper, this);
  }

  public static ColumnSpec fromBytes(ObjectMapper jsonMapper, byte[] bytes)
  {
    return CatalogSpecs.fromBytes(jsonMapper, bytes, ColumnSpec.class);
  }

  @Override
  public String toString()
  {
    return CatalogSpecs.toString(this);
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
    ColumnSpec other = (ColumnSpec) o;
    return Objects.equals(this.name, other.name)
        && Objects.equals(this.sqlType, other.sqlType);
  }
}
