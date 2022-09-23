package org.apache.druid.testsEx.catalog;

import org.apache.druid.catalog.TableId;
import org.apache.druid.catalog.TableMetadata;
import org.apache.druid.catalog.TableSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.http.CatalogResource;
import org.apache.druid.testsEx.cluster.DruidClusterClient;

public class CatalogClient
{
  private DruidClusterClient clusterClient;

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
}
