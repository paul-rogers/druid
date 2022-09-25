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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.catalog.CatalogSpecs;
import org.apache.druid.catalog.TableId;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.catalog.CatalogManager.TableState;

import java.util.Objects;

public class TableMetadata
{
  private final TableId id;
  private final long creationTime;
  private final long updateTime;
  private final TableState state;
  private final TableSpec spec;

  public TableMetadata(
      @JsonProperty("id") TableId tableId,
      @JsonProperty("creationTime") long creationTime,
      @JsonProperty("updateTime") long updateTime,
      @JsonProperty("state") TableState state,
      @JsonProperty("spec") TableSpec spec)
  {
    this.id = tableId;
    this.creationTime = creationTime;
    this.updateTime = updateTime;
    this.state = state;
    this.spec = spec;
  }

  public static TableMetadata newTable(
      TableId id,
      TableSpec defn
  )
  {
    return new TableMetadata(
        id,
        0,
        0,
        TableState.ACTIVE,
        defn
    );
  }

  public static TableMetadata newSegmentTable(
      String name,
      TableSpec defn
  )
  {
    return newTable(
        TableId.datasource(name),
        defn);
  }

  public TableMetadata asUpdate(long updateTime)
  {
    return new TableMetadata(
        id,
        creationTime,
        updateTime,
        state,
        spec);
  }

  public TableMetadata withSpec(TableSpec spec)
  {
    return new TableMetadata(
        id,
        creationTime,
        updateTime,
        state,
        spec
    );
  }

  @JsonProperty("id")
  public TableId id()
  {
    return id;
  }

  public String sqlName()
  {
    return id.sqlName();
  }

  @JsonProperty("state")
  public TableState state()
  {
    return state;
  }

  @JsonProperty("creationTime")
  public long creationTime()
  {
    return creationTime;
  }

  @JsonProperty("updateTime")
  public long updateTime()
  {
    return updateTime;
  }

  @JsonProperty("spec")
  public TableSpec spec()
  {
    return spec;
  }

  /**
   * Syntactic validation of a table object. Validates only that which
   * can be checked from this table object.
   */
  public void validate()
  {
    if (Strings.isNullOrEmpty(id.schema())) {
      throw new IAE("Database schema is required");
    }
    if (Strings.isNullOrEmpty(id.name())) {
      throw new IAE("Table name is required");
    }
    if (spec == null) {
      throw new IAE("A table definition must include a table spec.");
    }
  }

  public byte[] toBytes(ObjectMapper jsonMapper)
  {
    return CatalogSpecs.toBytes(jsonMapper, this);
  }

  public static TableMetadata fromBytes(ObjectMapper jsonMapper, byte[] bytes)
  {
    return CatalogSpecs.fromBytes(jsonMapper, bytes, TableMetadata.class);
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
    TableMetadata other = (TableMetadata) o;
    return Objects.equals(id, other.id)
        && creationTime == other.creationTime
        && updateTime == other.updateTime
        && state == other.state
        && Objects.equals(spec, other.spec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        id,
        creationTime,
        updateTime,
        state,
        spec
    );
  }
}