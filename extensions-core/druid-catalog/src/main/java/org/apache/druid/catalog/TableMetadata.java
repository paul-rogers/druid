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
import org.apache.druid.guice.annotations.PublicApi;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.catalog.CatalogManager.TableState;

import java.util.Objects;

/**
 * REST API level description of a table. Tables have multiple types
 * as described by subclasses.
 */
@PublicApi
public class TableMetadata
{
  public enum TableType
  {
    DATASOURCE,
    INPUT,
    VIEW,
    TOMBSTONE
  }

  private final String dbSchema;
  private final String name;
  private final long creationTime;
  private final long updateTime;
  private final TableState state;
  private final TableSpec spec;

  public TableMetadata(
      @JsonProperty("dbSchema") String dbSchema,
      @JsonProperty("name") String name,
      @JsonProperty("creationTime") long creationTime,
      @JsonProperty("updateTime") long updateTime,
      @JsonProperty("state") TableState state,
      @JsonProperty("spec") TableSpec spec)
  {
    this.dbSchema = dbSchema;
    this.name = name;
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
    return newTable(id.schema(), id.name(), defn);
  }

  public static TableMetadata newTable(
      String dbSchema,
      String name,
      TableSpec defn
  )
  {
    return new TableMetadata(
        dbSchema,
        name,
        0,
        0,
        TableState.ACTIVE,
        defn);
  }

  public static TableMetadata newSegmentTable(
      String name,
      TableSpec defn
  )
  {
    return newTable(
        TableId.DRUID_SCHEMA,
        name,
        defn);
  }

  public TableMetadata fromInsert(String dbSchema, long updateTime)
  {
    return new TableMetadata(
        dbSchema,
        name,
        updateTime,
        updateTime,
        state,
        spec);
  }

  public TableMetadata asUpdate(long updateTime)
  {
    return new TableMetadata(
        dbSchema,
        name,
        creationTime,
        updateTime,
        state,
        spec);
  }

//  public TableMetadata withSchema(String dbSchema)
//  {
//    if (dbSchema.equals(this.dbSchema)) {
//      return this;
//    }
//    return new TableMetadata(
//        dbSchema,
//        name,
//        creationTime,
//        updateTime,
//        state,
//        spec);
//  }

  public TableMetadata withSpec(TableSpec spec)
  {
    return new TableMetadata(
      dbSchema,
      name,
      creationTime,
      updateTime,
      state,
      spec
    );
  }

  public TableId id()
  {
    return new TableId(resolveDbSchema(), name);
  }

  @JsonProperty("dbSchema")
  public String dbSchema()
  {
    return dbSchema;
  }

  @JsonProperty("name")
  public String name()
  {
    return name;
  }

  public String sqlName()
  {
    return StringUtils.format("\"%s\".\"%s\"", dbSchema, name);
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
    if (Strings.isNullOrEmpty(dbSchema)) {
      throw new IAE("Database schema is required");
    }
    if (Strings.isNullOrEmpty(name)) {
      throw new IAE("Table name is required");
    }
    if (spec == null) {
      throw new IAE("A table definition must include a table spec.");
    }
    spec.validate();
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

  public String resolveDbSchema()
  {
    if (!Strings.isNullOrEmpty(dbSchema)) {
      return dbSchema;
    } else if (spec != null) {
      return spec.defaultSchema();
    } else {
      return null;
    }
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
    return Objects.equals(dbSchema, other.dbSchema)
        && Objects.equals(name, other.name)
        && creationTime == other.creationTime
        && updateTime == other.updateTime
        && state == other.state
        && Objects.equals(spec, other.spec);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dbSchema,
        name,
        creationTime,
        updateTime,
        state,
        spec);
  }

  public TableType type()
  {
    return spec == null ? null : spec.type();
  }
}
