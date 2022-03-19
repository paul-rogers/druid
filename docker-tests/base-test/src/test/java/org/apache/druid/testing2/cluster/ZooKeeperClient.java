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

package org.apache.druid.testing2.cluster;

import org.apache.curator.ensemble.EnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.druid.curator.CuratorConfig;
import org.apache.druid.curator.CuratorModule;
import org.apache.druid.curator.ExhibitorConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.testing2.config.ClusterConfig;
import org.apache.druid.testing2.config.ZKConfig;

import java.util.concurrent.TimeUnit;

/**
 * Test oriented ZooKeeper client.
 */
public class ZooKeeperClient
{
  private final ClusterConfig clusterConfig;
  private final ZKConfig config;
  private CuratorFramework curatorFramework;

  public ZooKeeperClient(ClusterConfig config)
  {
    this.clusterConfig = config;
    this.config = config.zk();
    if (this.config == null) {
      throw new ISE("ZooKeeper not configured");
    }
  }

  /**
   * Ensure ZK is ready.
   */
  public void open()
  {
    prepare();
    awaitReady();
  }

  public void prepare()
  {
    CuratorConfig curatorConfig = clusterConfig.toCuratorConfig();
    ExhibitorConfig exhibitorConfig = clusterConfig.toExhibitorConfig();
    EnsembleProvider ensembleProvider = CuratorModule.createEnsembleProvider(curatorConfig, exhibitorConfig);
    curatorFramework = CuratorModule.createCurator(curatorConfig, ensembleProvider);
  }

  public void awaitReady()
  {
    int timeoutSec = config.startTimeoutSecs();
    if (timeoutSec == 0) {
      timeoutSec = 5;
    }
    try {
      curatorFramework.blockUntilConnected(timeoutSec, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      throw new ISE("ZooKeeper timed out waiting for connect");
    }
  }

  public CuratorFramework curator()
  {
    return curatorFramework;
  }

  public void close()
  {
    curatorFramework.close();
    curatorFramework = null;
  }
}
