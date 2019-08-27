/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.Optional;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.ReplicationPeerNotFoundException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfigBuilder;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.CONFIG)
public class CarbonMasterObserver implements MasterCoprocessor, MasterObserver {
  private static final Logger LOG = LoggerFactory.getLogger(CarbonMasterObserver.class);

  // Root path where hbase data will e written into Carbon format
  /* Replication peer ID for writing HBase writes into Carbon format */
  public static final String CARBON_REPLICATION_PEER = "carbon_replication_peer";
  /* Atrribute to be passed in table descriptor while creating/modifying a table */
  public static final String CARBON_SCHEMA_DESC = "CARBON_SCHEMA";

  /* Table property where hbase mapping will be defined for writing data into Carbon format */
  public static final String HBASE_MAPPING_DETAILS = "hbase_mapping";

  /* Table property where table path is defined*/
  public static final String PATH = "path";
  /* Carbon table id*/
  public static final String CARBON_TABLE_ID = "carbon_table_id";

  @Override public Optional<MasterObserver> getMasterObserver() {
    return Optional.of(this);
  }

  @Override public void start(final CoprocessorEnvironment env) throws IOException {
    if (!(env instanceof MasterCoprocessorEnvironment)) {
      throw new CoprocessorException("Must be loaded on a Master!");
    }
  }

  @Override public void postStartMaster(ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    // Check and initialize carbon peer if doesn't exist
    initCarbonPeer(ctx);
  }

  /**
   * Clean up, if any, when master is stopped.
   *
   * @param env environment
   */
  @Override public void stop(CoprocessorEnvironment env) throws IOException {
  }

  @Override public void postCreateTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableDescriptor desc, RegionInfo[] regions) throws IOException {
    //handle in leo command.
  }

  @Override public void postModifyTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      final TableName tableName, TableDescriptor oldDescriptor, TableDescriptor currentDescriptor)
      throws IOException {
    LOG.info("Carbon >>>>> postModifyTable");
    // TODO: Add the logic to write Carbon schema for existing table
    // Validate the descriptor whether Carbon schema is defined newly or modifying existing schema
    // Create/modify Carbon schema accordingly
  }

  @Override public void postDeleteTable(final ObserverContext<MasterCoprocessorEnvironment> ctx,
      TableName tableName) throws IOException {
    LOG.info("Carbon >>>>> postDeleteTable");
    // TODO: Add the logic to delete the Carbon schema on table delete
  }

  /*
   * Check and add a Carbon replication peer.
   */
  private void initCarbonPeer(final ObserverContext<MasterCoprocessorEnvironment> ctx)
      throws IOException {
    try (Connection conn = ConnectionFactory
        .createConnection(ctx.getEnvironment().getConfiguration()); Admin admin = conn.getAdmin()) {
      if (!checkPeerInitialized(admin)) {
        LOG.info("Creating Carbon replication peer id=" + CARBON_REPLICATION_PEER);
        ReplicationPeerConfigBuilder builder = ReplicationPeerConfig.newBuilder();
        builder.setClusterKey("");
        builder.setReplicationEndpointImpl(CarbonReplicationEndpoint.class.getName());
        admin.addReplicationPeer(CARBON_REPLICATION_PEER, builder.build());
      }
    }
  }

  /*
   * Check whether replication peer exist.
   */
  private boolean checkPeerInitialized(Admin admin) throws IOException {
    ReplicationPeerConfig peerConfig = null;
    try {
      peerConfig = admin.getReplicationPeerConfig(CARBON_REPLICATION_PEER);
    } catch (ReplicationPeerNotFoundException e) {
      LOG.debug(
          "Carbon replication peer id=" + CARBON_REPLICATION_PEER + " doesn't exist, creating...",
          e);
    }
    return peerConfig != null ? true : false;
  }

}
