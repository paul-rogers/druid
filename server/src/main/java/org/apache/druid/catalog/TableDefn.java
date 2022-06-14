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

import java.util.Map;

/**
 * Definition of a table "hint" in the metastore, between client and
 * Druid, and between Druid nodes.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = "datasource", value = DatasourceDefn.class),
    @Type(name = "input", value = InputSourceDefn.class),
    @Type(name = "tombstone", value = TableDefn.Tombstone.class),
})
public class TableDefn
{
  private final Map<String, Object> properties;

  public TableDefn(Map<String, Object> properties)
  {
    this.properties = properties == null ? ImmutableMap.of() : properties;
  }

  @JsonProperty("properties")
  @JsonInclude(Include.NON_NULL)
  public Map<String, Object> properties()
  {
    return properties;
  }

  public void validate()
  {
  }

  public byte[] toBytes(ObjectMapper jsonMapper)
  {
    return CatalogDefns.toBytes(jsonMapper, this);
  }

  public static TableDefn fromBytes(ObjectMapper jsonMapper, byte[] bytes)
  {
    return CatalogDefns.fromBytes(jsonMapper, bytes, TableDefn.class);
  }

  @Override
  public String toString()
  {
    return CatalogDefns.toString(this);
  }

  public String defaultSchema()
  {
    return null;
  }

  public static class Tombstone extends TableDefn
  {
    public Tombstone()
    {
      super(null);
    }
  }
}
