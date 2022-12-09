/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.commit.hive;

import org.apache.flink.core.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.RetryingMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class HiveTableMetaStore {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTableMetaStore.class);

  private final String database;

  private final String tableName;

  private IMetaStoreClient client;

  private final StorageDescriptor sd;

  public HiveTableMetaStore(String database, String tableName) throws TException {
    client = getHiveMetaStoreClient();
    sd = client.getTable(database, tableName).getSd();
    this.database = database;
    this.tableName = tableName;
  }

  private IMetaStoreClient getHiveMetaStoreClient() throws MetaException {
    HiveConf hiveConf = getHiveConfig();
    // set test hive metastore
    // hiveConf.set("hive.metastore.uris","thrift://10.70.82.11:9083");
    LOG.info("hive metastore uris " + hiveConf.get("hive.metastore.uris"));
    client = RetryingMetaStoreClient.getProxy(hiveConf, false);
    return client;
  }

  private HiveConf getHiveConfig() {
    try {
      String confDir = System.getenv("HADOOP_CONF_DIR");
      if (new File(confDir).exists()) {
        LOG.info("load hive site path : " + confDir);
        HiveConf hiveConf = new HiveConf();
        hiveConf.addResource(new FileInputStream(confDir + "/hive-site.xml"));
        return hiveConf;
      } else {
        throw new RuntimeException(String.format("%s not load hive site file", confDir));
      }
    } catch (Exception e) {
      LOG.error("load hive site path error", e);
      throw new RuntimeException(e);
    }
  }

  public void createOrAlterPartition(LinkedHashMap<String, String> partitionSpec, Path partitionPath, boolean isCompletingPartitionWrite) throws Exception {
    Partition partition;
    try {
      // partition = client.getPartition(database, tableName, new ArrayList<>(partitionSpec.values()));
      partition = client.getPartition(database, tableName, new ArrayList<>(partitionSpec.values()), true);
    } catch (NoSuchObjectException e) {
      LOG.warn("Failed to get partition, database = {}, tableName = {}, partition = {}",
          database, tableName, partitionSpec.values(), e);
      createPartition(partitionSpec, partitionPath, isCompletingPartitionWrite);
      return;
    }
    LOG.info("Commit partition success, and the partition is {}.", partition.toString());
    alterPartition(partitionPath, partition, isCompletingPartitionWrite);
  }

  private void createPartition(LinkedHashMap<String, String> partSpec, Path path, boolean isCompletingPartitionWrite) throws Exception {
    StorageDescriptor newSd = new StorageDescriptor(sd);
    newSd.setLocation(path.toString());
    Partition partition = HiveTableUtil.createHivePartition(database, tableName, new ArrayList<>(partSpec.values()), newSd, new HashMap<>());
    partition.setValues(new ArrayList<>(partSpec.values()));
    Map<String, String> parameters = new HashMap<>();
    parameters.put("commit", String.valueOf(isCompletingPartitionWrite));
    partition.setParameters(parameters);
    client.add_partition(partition);
  }

  private void alterPartition(Path partitionPath, Partition currentPartition, boolean isCompletingPartitionWrite) throws Exception {
    StorageDescriptor partSD = currentPartition.getSd();
    // the following logic copied from Hive::alterPartitionSpecInMemory
    partSD.setOutputFormat(sd.getOutputFormat());
    partSD.setInputFormat(sd.getInputFormat());
    partSD.getSerdeInfo().setSerializationLib(sd.getSerdeInfo().getSerializationLib());
    partSD.getSerdeInfo().setParameters(sd.getSerdeInfo().getParameters());
    partSD.setBucketCols(sd.getBucketCols());
    partSD.setNumBuckets(sd.getNumBuckets());
    partSD.setSortCols(sd.getSortCols());
    partSD.setLocation(partitionPath.toString());
    Map<String, String> parameters = new HashMap<>();
    parameters.put("commit", String.valueOf(isCompletingPartitionWrite));
    currentPartition.setParameters(parameters);
    client.alter_partition(database, tableName, currentPartition);
  }

  public void close() {
    client.close();
  }

  public static class HiveTableMetaStoreFactory {

    public static volatile HiveTableMetaStore hiveTableMetaStore;

    public static HiveTableMetaStore getHiveTableMetaStore(String database, String tableName) throws TException {
      if (hiveTableMetaStore == null) {
        synchronized (HiveTableMetaStoreFactory.class) {
          if (hiveTableMetaStore == null) {
            hiveTableMetaStore = new HiveTableMetaStore(database, tableName);
          }
        }
      }
      return hiveTableMetaStore;
    }
  }
}
