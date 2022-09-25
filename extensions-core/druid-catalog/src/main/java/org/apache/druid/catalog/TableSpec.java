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
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.TableMetadata.TableType;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.java.util.common.UOE;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Definition of a table "hint" in the metastore, between client and
 * Druid, and between Druid nodes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = DatasourceSpec.JSON_TYPE, value = DatasourceSpec.class),
    @Type(name = InputTableSpec.JSON_TYPE, value = InputTableSpec.class),
    @Type(name = TableSpec.Tombstone.JSON_TYPE, value = TableSpec.Tombstone.class),
})
@UnstableApi
public abstract class TableSpec
{
  private final Map<String, Object> tags;

  public TableSpec(Map<String, Object> tags)
  {
    this.tags = tags == null ? ImmutableMap.of() : tags;
  }

  @JsonProperty("tags")
  @JsonInclude(Include.NON_NULL)
  public Map<String, Object> tags()
  {
    return tags;
  }

  public void validate()
  {
  }

  public abstract TableType type();

  /**
   * For updates, merge the given update with the existing spec,
   * giving a new, merged, spec.
   */
  public abstract TableSpec merge(TableSpec update, Map<String, Object> raw, ObjectMapper mapper);

  public byte[] toBytes(ObjectMapper jsonMapper)
  {
    return CatalogSpecs.toBytes(jsonMapper, this);
  }

  public static TableSpec fromBytes(ObjectMapper jsonMapper, byte[] bytes)
  {
    return CatalogSpecs.fromBytes(jsonMapper, bytes, TableSpec.class);
  }

  @Override
  public String toString()
  {
    return CatalogSpecs.toString(this);
  }

  public String defaultSchema()
  {
    return null;
  }

  public abstract List<? extends ColumnSpec> columnSpecs();

  /**
   * Internal class used in updates to notify listeners that a table has
   * been deleted. Avoids the need for a special "table deleted" message.
   */
  public static class Tombstone extends TableSpec
  {
    public static final String JSON_TYPE = "tombstone";

    public Tombstone()
    {
      super(null);
    }

    @Override
    public TableType type()
    {
      return TableType.TOMBSTONE;
    }

    @Override
    public TableSpec merge(TableSpec update, Map<String, Object> raw, ObjectMapper mapper)
    {
      throw new UOE("Tombstones should not exist in the catalog.");
    }

    @Override
    public List<? extends ColumnSpec> columnSpecs()
    {
      return Collections.emptyList();
    }
  }
}
