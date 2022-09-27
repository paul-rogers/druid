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

package org.apache.druid.catalog.specs.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.CatalogObjectFacade;
import org.apache.druid.catalog.specs.TableDefn;
import org.apache.druid.catalog.specs.TableSpec;
import org.apache.druid.java.util.common.IAE;

import java.util.Map;

public class CatalogTableRegistry
{
  public static class ResolvedTable extends CatalogObjectFacade
  {
    private final TableDefn defn;
    private final TableSpec spec;
    private final ObjectMapper jsonMapper;

    public ResolvedTable(
        final TableDefn defn,
        final TableSpec spec,
        final ObjectMapper jsonMapper
    )
    {
      this.defn = defn;
      this.spec = spec;
      this.jsonMapper = jsonMapper;
    }

    public TableDefn defn()
    {
      return defn;
    }

    public TableSpec spec()
    {
      return spec;
    }

    public ResolvedTable merge(TableSpec update)
    {
      return new ResolvedTable(
          defn,
          defn.merge(spec, update, jsonMapper),
          jsonMapper
      );
    }

    public ResolvedTable withProperties(Map<String, Object> props)
    {
      return new ResolvedTable(defn, spec.withProperties(props), jsonMapper);
    }

    public void validate()
    {
      defn.validate(this);
    }

    @Override
    public Map<String, Object> properties()
    {
      return spec.properties();
    }

    public ObjectMapper jsonMapper()
    {
      return jsonMapper;
    }
  }

  public static final TableDefn DETAIL_DATASOURCE_DEFN = new DatasourceDefn.DetailDatasourceDefn();
  public static final TableDefn ROLLUP_DATASOURCE_DEFN = new DatasourceDefn.RollupDatasourceDefn();

  private static final TableDefn[] TABLE_DEFNS = {
      DETAIL_DATASOURCE_DEFN,
      ROLLUP_DATASOURCE_DEFN
  };

  private final Map<String, TableDefn> defns;
  private final ObjectMapper jsonMapper;

  public CatalogTableRegistry(
      final TableDefn[] defns,
      final ObjectMapper jsonMapper
  )
  {
    ImmutableMap.Builder<String, TableDefn> builder = ImmutableMap.builder();
    for (TableDefn defn : defns) {
      builder.put(defn.typeValue(), defn);
    }
    this.defns = builder.build();
    this.jsonMapper = jsonMapper;
  }

  public CatalogTableRegistry(
      final ObjectMapper jsonMapper
  )
  {
    this(TABLE_DEFNS, jsonMapper);
  }

  public ResolvedTable resolve(TableSpec spec)
  {
    String type = spec.type();
    if (Strings.isNullOrEmpty(type)) {
      throw new IAE("The table type is required.");
    }
    TableDefn defn = defns.get(type);
    if (defn == null) {
      throw new IAE("Table type [%s] is not valid.", type);
    }
    return new CatalogTableRegistry.ResolvedTable(defn, spec, jsonMapper);
  }
}