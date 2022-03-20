package org.apache.druid.tests.leadership;

import com.google.inject.Inject;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.StatusResponseHandler;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.apache.druid.testing2.utils.DruidClusterAdminClient;
import org.apache.druid.testing.utils.SqlTestQueryHelper;
import org.apache.druid.testing2.config.Initializer;
import org.apache.druid.tests.indexer.AbstractIndexerTest;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.URL;

import static org.junit.Assert.assertNotEquals;

public class TempTest2
{
  private static final String SYSTEM_QUERIES_RESOURCE = "/queries/high_availability_sys.json";
  private static final int NUM_LEADERSHIP_SWAPS = 3;

  private static Initializer initializer;

  @Inject
  private IntegrationTestingConfig config;

  @Inject
  private DruidClusterAdminClient druidClusterAdminClient;

  @Inject
  SqlTestQueryHelper queryHelper;

  @Inject
  @TestClient
  HttpClient httpClient;

  @BeforeClass
  public static void setup()
  {
    initializer = new Initializer();
  }

  public TempTest2()
  {
    initializer.injector().injectMembers(this);
  }

  @Test
  public void testLeadershipChanges() throws Exception
  {
    int runCount = 0;
    String previousCoordinatorLeader = null;
    String previousOverlordLeader = null;
    // fetch current leaders, make sure queries work, then swap leaders and do it again
    do {
      String coordinatorLeader = getLeader("coordinator");
      String overlordLeader = getLeader("indexer");

      // we expect leadership swap to happen
      assertNotEquals(previousCoordinatorLeader, coordinatorLeader);
      assertNotEquals(previousOverlordLeader, overlordLeader);

      previousCoordinatorLeader = coordinatorLeader;
      previousOverlordLeader = overlordLeader;

      String queries = fillTemplate(
          config,
          AbstractIndexerTest.getResourceAsString(SYSTEM_QUERIES_RESOURCE),
          overlordLeader,
          coordinatorLeader
      );
      queryHelper.testQueriesFromString(queries);

      swapLeadersAndWait(coordinatorLeader, overlordLeader);
    } while (runCount++ < NUM_LEADERSHIP_SWAPS);
  }

  private void swapLeadersAndWait(String coordinatorLeader, String overlordLeader)
  {
    Runnable waitUntilCoordinatorSupplier;
    if (isCoordinatorOneLeader(config, coordinatorLeader)) {
      druidClusterAdminClient.restartCoordinatorContainer();
      waitUntilCoordinatorSupplier = () -> druidClusterAdminClient.waitUntilCoordinatorReady();
    } else {
      druidClusterAdminClient.restartCoordinatorTwoContainer();
      waitUntilCoordinatorSupplier = () -> druidClusterAdminClient.waitUntilCoordinatorTwoReady();
    }

    Runnable waitUntilOverlordSupplier;
    if (isOverlordOneLeader(config, overlordLeader)) {
      druidClusterAdminClient.restartOverlordContainer();
      waitUntilOverlordSupplier = () -> druidClusterAdminClient.waitUntilIndexerReady();
    } else {
      druidClusterAdminClient.restartOverlordTwoContainer();
      waitUntilOverlordSupplier = () -> druidClusterAdminClient.waitUntilOverlordTwoReady();
    }
    waitUntilCoordinatorSupplier.run();
    waitUntilOverlordSupplier.run();
  }

  private String getLeader(String service)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.GET,
              new URL(StringUtils.format(
                  "%s/druid/%s/v1/leader",
                  config.getRouterUrl(),
                  service
              ))
          ),
          StatusResponseHandler.getInstance()
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while fetching leader from[%s] status[%s] content[%s]",
            config.getRouterUrl(),
            response.getStatus(),
            response.getContent()
        );
      }
      return response.getContent();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String fillTemplate(IntegrationTestingConfig config, String template, String overlordLeader, String coordinatorLeader)
  {
    /*
      {"host":"%%BROKER%%","server_type":"broker", "is_leader": %%NON_LEADER%%},
      {"host":"%%COORDINATOR_ONE%%","server_type":"coordinator", "is_leader": %%COORDINATOR_ONE_LEADER%%},
      {"host":"%%COORDINATOR_TWO%%","server_type":"coordinator", "is_leader": %%COORDINATOR_TWO_LEADER%%},
      {"host":"%%OVERLORD_ONE%%","server_type":"overlord", "is_leader": %%OVERLORD_ONE_LEADER%%},
      {"host":"%%OVERLORD_TWO%%","server_type":"overlord", "is_leader": %%OVERLORD_TWO_LEADER%%},
      {"host":"%%ROUTER%%","server_type":"router", "is_leader": %%NON_LEADER%%}
     */
    String working = template;

    working = StringUtils.replace(working, "%%OVERLORD_ONE%%", config.getOverlordInternalHost());
    working = StringUtils.replace(working, "%%OVERLORD_TWO%%", config.getOverlordTwoInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_ONE%%", config.getCoordinatorInternalHost());
    working = StringUtils.replace(working, "%%COORDINATOR_TWO%%", config.getCoordinatorTwoInternalHost());
    working = StringUtils.replace(working, "%%BROKER%%", config.getBrokerInternalHost());
    working = StringUtils.replace(working, "%%ROUTER%%", config.getRouterInternalHost());
    if (isOverlordOneLeader(config, overlordLeader)) {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%OVERLORD_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%OVERLORD_TWO_LEADER%%", "1");
    }
    if (isCoordinatorOneLeader(config, coordinatorLeader)) {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "1");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "0");
    } else {
      working = StringUtils.replace(working, "%%COORDINATOR_ONE_LEADER%%", "0");
      working = StringUtils.replace(working, "%%COORDINATOR_TWO_LEADER%%", "1");
    }
    working = StringUtils.replace(working, "%%NON_LEADER%%", String.valueOf(NullHandling.defaultLongValue()));
    return working;
  }

  private static boolean isCoordinatorOneLeader(IntegrationTestingConfig config, String coordinatorLeader)
  {
    return coordinatorLeader.contains(transformHost(config.getCoordinatorInternalHost()));
  }

  private static boolean isOverlordOneLeader(IntegrationTestingConfig config, String overlordLeader)
  {
    return overlordLeader.contains(transformHost(config.getOverlordInternalHost()));
  }

  /**
   * host + ':' which should be enough to distinguish subsets, e.g. 'druid-coordinator:8081' from
   * 'druid-coordinator-two:8081' for example
   */
  private static String transformHost(String host)
  {
    return StringUtils.format("%s:", host);
  }

}
