package org.apache.druid.test;

import com.google.inject.Injector;
import org.apache.druid.sql.http.SqlQuery;
import org.apache.druid.testing.clients.SqlResourceTestClient;
import org.apache.druid.testing2.config.GuiceConfig;
import org.apache.druid.testing2.utils.DruidClusterAdminClient;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TestClients
{

  @Test
  public void testAdminClient()
  {
    GuiceConfig guiceConfig = new GuiceConfig();
    Injector injector = guiceConfig.injector();
    DruidClusterAdminClient druidClusterAdminClient = injector.getInstance(DruidClusterAdminClient.class);
  }

  @Test
  public void testQueryClient()
  {
    GuiceConfig guiceConfig = new GuiceConfig();
    Injector injector = guiceConfig.injector();
    SqlResourceTestClient client = injector.getInstance(SqlResourceTestClient.class);
    String url = "http://localhost:8082/druid/v2/sql";
    String stmt = "SELECT COUNT(*) AS cnt FROM sys.servers";
    SqlQuery query = new SqlQuery(stmt, null, false, false, false, null, null);
    List<Map<String, Object>> results = client.query(url, query);
    assertEquals(1, results.size());
    assertTrue((Integer) results.get(0).get("cnt") > 1);
  }
}
