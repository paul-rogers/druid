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

package org.apache.druid.testsEx.cluster;

import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.druid.catalog.specs.TableId;
import org.apache.druid.catalog.storage.HideColumns;
import org.apache.druid.catalog.storage.MoveColumn;
import org.apache.druid.catalog.storage.TableMetadata;
import org.apache.druid.catalog.storage.TableSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.http.CatalogResource;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.List;
import java.util.Map;

public class CatalogClient
{
  private final DruidClusterClient clusterClient;

  public CatalogClient(final DruidClusterClient clusterClient)
  {
    this.clusterClient = clusterClient;
  }

  public long createTable(TableId tableId, TableSpec spec)
  {
    String url = StringUtils.format(
        "%s/%s/tables/%s/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    return clusterClient.post(url, spec, Long.class);
  }

  public long updateTable(TableId tableId, Map<String, Object> updates, long version)
  {
    String url = StringUtils.format(
        "%s/%s/tables/%s/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    if (version > 0) {
      url += "?version=" + version;
    }
    return clusterClient.put(url, updates, Long.class);
  }

  public TableMetadata readTable(TableId tableId)
  {
    String url = StringUtils.format(
        "%s/%s/tables/%s/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    return clusterClient.getAs(url, TableMetadata.class);
  }

  public void dropTable(TableId tableId)
  {
    String url = StringUtils.format(
        "%s/%s/tables/%s/%s",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    clusterClient.send(HttpMethod.DELETE, url);
  }

  public long moveColumn(TableId tableId, MoveColumn cmd)
  {
    String url = StringUtils.format(
        "%s/%s/tables/%s/%s/moveColumn",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    return clusterClient.post(url, cmd, Long.class);
  }

  public long hideColumns(TableId tableId, HideColumns cmd)
  {
    String url = StringUtils.format(
        "%s/%s/tables/%s/%s/hideColumns",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    return clusterClient.post(url, cmd, Long.class);
  }

  public long dropColumns(TableId tableId, List<String> columns)
  {
    String url = StringUtils.format(
        "%s/%s/tables/%s/%s/dropColumns",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        tableId.schema(),
        tableId.name()
    );
    return clusterClient.post(url, columns, Long.class);
  }

  public List<String> listSchemas()
  {
    String url = StringUtils.format(
        "%s/%s/list/schemas/names",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH
    );
    return clusterClient.getAs(url, new TypeReference<List<String>>() { });
  }

  public List<TableId> listTables()
  {
    String url = StringUtils.format(
        "%s/%s/list/tables/names",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH
    );
    return clusterClient.getAs(url, new TypeReference<List<TableId>>() { });
  }

  public List<String> listTableNamesInSchema(String schemaName)
  {
    String url = StringUtils.format(
        "%s/%s/schemas/%s/names",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        schemaName
    );
    return clusterClient.getAs(url, new TypeReference<List<String>>() { });
  }

  public List<TableMetadata> listTablesInSchema(String schemaName)
  {
    String url = StringUtils.format(
        "%s/%s/schemas/%s/tables",
        clusterClient.leadCoordinatorUrl(),
        CatalogResource.ROOT_PATH,
        schemaName
    );
    return clusterClient.getAs(url, new TypeReference<List<TableMetadata>>() { });
  }
}
