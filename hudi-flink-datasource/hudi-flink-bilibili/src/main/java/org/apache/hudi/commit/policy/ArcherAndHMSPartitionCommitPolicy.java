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

package org.apache.hudi.commit.policy;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.commit.archer.ArcherOperatorFactory;
import org.apache.hudi.commit.archer.configuration.ArcherOptions;
import org.apache.hudi.commit.hive.HiveTableMetaStore;
import org.apache.hudi.commit.serializer.PartitionStateSerializer;
import org.apache.hudi.commit.util.MapUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.sink.CommitType;
import org.apache.hudi.sink.state.CoordinatorState;
import org.apache.hudi.sink.utils.DateTimeUtils;

import org.apache.flink.bili.external.archer.ArcherOperator;
import org.apache.flink.bili.external.archer.constant.ArcherConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.table.utils.PartitionPathUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class ArcherAndHMSPartitionCommitPolicy implements PartitionCommitPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(ArcherAndHMSPartitionCommitPolicy.class);

  private Configuration conf;
  private ArcherOperator archerOperator;
  private List<String> archerPartitionField;
  private PartitionState partitionState;
  private SimpleVersionedSerializer<PartitionState> serializer;
  private List<String> partitionField = new ArrayList<>();

  @Override
  public void init(Configuration conf) {
    this.conf = conf;
    initArcherOperator();
  }

  private void initArcherOperator() {
    if (conf.getBoolean(ArcherOptions.ARCHER_COMMIT_ENABLED)) {
      Map<String, String> config = new HashMap<>();
      config.put(ArcherConstants.SINK_DATABASE_NAME_KEY, conf.getString(ArcherOptions.ARCHER_COMMIT_DB));
      config.put(ArcherConstants.SINK_TABLE_NAME_KEY, conf.getString(ArcherOptions.ARCHER_COMMIT_TABLE));
      config.put(ArcherConstants.SYSTEM_USER_ID, conf.getString(ArcherOptions.ARCHER_COMMIT_USER));
      this.archerOperator = ArcherOperatorFactory.getArcherOperatorInstance(MapUtils.toProperties(config));
      String archerCommitType = conf.getString(ArcherOptions.ARCHER_COMMIT_TYPE).toUpperCase();
      if (CommitType.LOG_DATE.name().equals(archerCommitType)) {
        List<String> archerPartition = new ArrayList<>();
        archerPartition.add(CommitType.LOG_DATE.toString().toLowerCase());
        this.archerPartitionField = archerPartition;
      } else if (CommitType.LOG_HOUR.name().equals(archerCommitType)) {
        List<String> archerPartition = new ArrayList<>();
        archerPartition.add(CommitType.LOG_DATE.toString().toLowerCase());
        archerPartition.add(CommitType.LOG_HOUR.toString().toLowerCase());
        this.archerPartitionField = archerPartition;
      }
    }
    partitionField.addAll(Arrays.asList(conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_FIELDS).split(",")));
    this.serializer = PartitionStateSerializer.INSTANCE;
  }

  @Override
  public byte[] stateSerialize(CoordinatorState coordinatorState) throws IOException {
    return this.serializer.serialize((PartitionState) coordinatorState);
  }

  @Override
  public CoordinatorState stateDeserialize(byte[] checkpointData) throws IOException {
    return this.serializer.deserialize(serializer.getVersion(), checkpointData);
  }

  @Override
  public CoordinatorState createState() {
    this.partitionState = PartitionState.create();
    return partitionState;
  }

  @Override
  public CoordinatorState getCurrentState() {
    return this.partitionState;
  }

  @Override
  public void setCurrentState(CoordinatorState state) {
    this.partitionState = (PartitionState) state;
  }

  @Override
  public boolean commitNotice(List writeResults, long watermark) {
    try {
      commitToHms(writeResults, watermark);
      commitToArcher(writeResults, watermark);
    } catch (Exception e) {
      LOG.error("partition commit notice error, ", e);
      return false;
    }
    return true;
  }

  public void commitToHms(List<WriteStatus> writeResults, long watermark) {
    LOG.info("hive sync enable flag {}, and the partition fields is {}, and the writeResult is {}, and the low watermark is {}",
        conf.getBoolean(FlinkOptions.HIVE_SYNC_ENABLED),
        conf.getString(FlinkOptions.HIVE_SYNC_PARTITION_FIELDS),
        writeResults.stream().map(WriteStatus::toString).collect(Collectors.joining("|")),
        watermark);
    if (watermark == Long.MAX_VALUE) {
      LOG.warn("watermark is max value skipping commits");
      return;
    }

    if (conf.getBoolean(FlinkOptions.HIVE_SYNC_ENABLED)) {
      try {

        long partitionInterval = partitionField.size() == 1
            ? DateTimeUtils.DAY_INTERVAL : DateTimeUtils.HOUR_INTERVAL;
        String commitType = partitionField.size() == 1
            ? CommitType.LOG_DATE.name() : CommitType.LOG_HOUR.name();
        TreeMap<Long, String> writePartitionCollector = getWritePartitions(writeResults);
        Map.Entry<Long, String> minPartition = writePartitionCollector.firstEntry();
        Map.Entry<Long, String> maxPartition = writePartitionCollector.lastEntry();
        long hiveBeforeCommitStartTime = partitionState.lastHiveBeforeCommitTime;
        long hiveAfterCommitStartTime = partitionState.lastHiveAfterCommitTime;
        long hiveAfterCommitEndTime = maxPartition.getKey();
        long hiveBeforeCommitEndTime = maxPartition.getKey();

        if (hiveBeforeCommitStartTime == 0) {
          hiveBeforeCommitStartTime = minPartition.getKey();
        }
        if (hiveAfterCommitStartTime == 0) {
          hiveAfterCommitStartTime = minPartition.getKey();
        }
        if (hiveAfterCommitStartTime > watermark && watermark != Long.MIN_VALUE) {
          hiveAfterCommitEndTime = watermark;
        }
        if (hiveBeforeCommitEndTime > watermark && watermark != Long.MIN_VALUE) {
          hiveBeforeCommitEndTime = watermark;
        }

        hiveBeforeCommitEndTime = waterMarkToCurrentPartitionTime(hiveBeforeCommitEndTime, commitType);
        hiveAfterCommitEndTime = waterMarkToCurrentPartitionTime(watermark == Long.MIN_VALUE
            ? hiveAfterCommitEndTime : hiveAfterCommitEndTime - partitionInterval, commitType);
        List<List<String>> hiveBeforeNeedCommit = getNeedCommitList(hiveBeforeCommitStartTime, hiveBeforeCommitEndTime, partitionInterval, commitType);
        List<List<String>> hiveAfterNeedCommit = getNeedCommitList(hiveAfterCommitStartTime, hiveAfterCommitEndTime, partitionInterval, commitType);
        boolean commitBefore = commitPartitionToHms(hiveBeforeNeedCommit, false);
        boolean commitAfter = commitPartitionToHms(hiveAfterNeedCommit, true);
        if (commitBefore && commitAfter) {
          this.partitionState.lastHiveAfterCommitTime = hiveAfterCommitEndTime + partitionInterval;
          this.partitionState.lastHiveBeforeCommitTime = hiveBeforeCommitEndTime + partitionInterval;
        }
        LOG.info("commit to hive partitions successful , lastHiveAfterCommitTime: {} , lastHiveBeforeCommitTime: {} ,watermark: {}",
            partitionState.lastArcherCommitTime,
            partitionState.lastHiveBeforeCommitTime,
            watermark);
      } catch (Exception e) {
        LOG.error("hive sync error", e);
      }
    }
  }

  public TreeMap getWritePartitions(List<WriteStatus> writeResults) throws ParseException {
    TreeMap<Long, String> writePartitionCollector = new TreeMap<Long, String>();
    for (WriteStatus writerResult : writeResults) {
      String partitionsPath = writerResult.getPartitionPath();
      if (writerResult.getPartitionPath().lastIndexOf('/') == partitionsPath.length() - 1) {
        partitionsPath = partitionsPath.substring(0, partitionsPath.lastIndexOf('/'));
      }
      List<String> partitionValues = PartitionPathUtils.extractPartitionValues(new Path(partitionsPath));
      if (partitionValues.size() == 1) {
        long partitionTime = DateTimeUtils.string2Time(partitionValues.get(0), DateTimeUtils.YYYYMMDD);
        writePartitionCollector.put(partitionTime, partitionsPath);
      } else if (partitionValues.size() == 2) {
        long partitionTime = DateTimeUtils.string2Time(partitionValues.get(0) + partitionValues.get(1), DateTimeUtils.YYYYMMDDHH);
        writePartitionCollector.put(partitionTime, partitionsPath);
      } else {
        throw new RuntimeException(String.format("extract partition error , partition is %s", partitionsPath));
      }
    }
    return writePartitionCollector;
  }

  public long waterMarkToCurrentPartitionTime(long time, String commitType) throws ParseException {
    if (CommitType.LOG_DATE.name().equals(commitType)) {
      String dateTimeStr = DateTimeUtils.time2String(time, DateTimeUtils.YYYYMMDD);
      return DateTimeUtils.string2Time(dateTimeStr, DateTimeUtils.YYYYMMDD);
    } else if (CommitType.LOG_HOUR.name().equals(commitType)) {
      String dateTimeStr = DateTimeUtils.time2String(time, DateTimeUtils.YYYYMMDDHH);
      return DateTimeUtils.string2Time(dateTimeStr, DateTimeUtils.YYYYMMDDHH);
    } else {
      throw new RuntimeException("commitType must is log_date or log_hour");
    }
  }

  public List<List<String>> getNeedCommitList(long commitStartTime, long commitEndTime, long partitionInterval, String type) {
    List<List<String>> needCommit = new LinkedList<List<String>>();
    long commitCurrentTime = commitStartTime;
    while (commitCurrentTime <= commitEndTime) {
      List<String> partitionValues = new LinkedList<String>();
      if (CommitType.LOG_DATE.name().equals(type.toUpperCase())) {
        String commitCurrentDateStr = DateTimeUtils.time2String(commitCurrentTime, DateTimeUtils.YYYYMMDD);
        partitionValues.add(commitCurrentDateStr);
      } else if (CommitType.LOG_HOUR.name().equals(type.toUpperCase())) {
        String commitCurrentDateStr = DateTimeUtils.time2String(commitCurrentTime, DateTimeUtils.YYYYMMDD_HH);
        partitionValues.addAll(Arrays.asList(commitCurrentDateStr.split("_")));
      }
      needCommit.add(partitionValues);
      commitCurrentTime = commitCurrentTime + partitionInterval;
    }
    return needCommit;
  }

  public boolean commitPartitionToHms(List<List<String>> needCommit, boolean isCompletingPartitionWrite) {
    try {
      HiveTableMetaStore metaStore = HiveTableMetaStore.HiveTableMetaStoreFactory
              .getHiveTableMetaStore(conf.getString(FlinkOptions.HIVE_SYNC_DB), conf.getString(FlinkOptions.HIVE_SYNC_TABLE));
      for (List<String> commitPartition : needCommit) {
        LinkedHashMap<String, String> partSpec = toPartSpec(commitPartition);

        String basePath = conf.getString(FlinkOptions.PATH);
        org.apache.flink.core.fs.Path path = new Path(basePath + "/" + toPartitionPath(commitPartition));
        if (!isCompletingPartitionWrite) {
          LOG.info("path is {}, need commit is {}, commit type before", path, needCommit);
          metaStore.createOrAlterPartition(partSpec, path, isCompletingPartitionWrite);
        } else {
          LOG.info("path is {}, need commit is {}, commit type after", path, needCommit);
          metaStore.createOrAlterPartition(partSpec, path, isCompletingPartitionWrite);
        }
      }
      return true;
    } catch (Exception e) {
      LOG.error(String.format("Failed to before commit partition %s", needCommit), e);
      return false;
    }
  }

  public String toPartitionPath(List<String> partitionValue) {
    StringBuffer partitionPath = new StringBuffer();
    for (int i = 0; i < partitionValue.size(); i++) {
      partitionPath.append(partitionField.get(i) + "=" + partitionValue.get(i));
      if (i != partitionValue.size() - 1) {
        partitionPath.append("/");
      }
    }
    return partitionPath.toString();
  }

  public LinkedHashMap<String, String> toPartSpec(List<String> partitionValue) {
    LinkedHashMap<String, String> partSpec = new LinkedHashMap<String, String>();
    for (int i = 0; i < partitionValue.size(); i++) {
      partSpec.put(partitionField.get(i), partitionValue.get(i));
    }
    return partSpec;
  }

  public void commitToArcher(List<WriteStatus> writeResults, long watermark) {
    LOG.info("archer enable flag {}, and the partition commmit type is {}, and the writeResult is {}, and the low watermark is {}",
        conf.getBoolean(ArcherOptions.ARCHER_COMMIT_ENABLED),
        conf.getString(ArcherOptions.ARCHER_COMMIT_TYPE),
        writeResults.stream().map(WriteStatus::toString).collect(Collectors.joining("|")),
        watermark);
    if (watermark == Long.MAX_VALUE) {
      LOG.warn("watermark is max value skipping commits");
      return;
    }

    if (conf.getBoolean(ArcherOptions.ARCHER_COMMIT_ENABLED)) {
      try {
        String database = conf.getString(ArcherOptions.ARCHER_COMMIT_DB);
        String table = conf.getString(ArcherOptions.ARCHER_COMMIT_TABLE);
        String archerCommitType = conf.getString(ArcherOptions.ARCHER_COMMIT_TYPE).toUpperCase();
        long partitionInterval = archerPartitionField.size() == 1
            ? DateTimeUtils.DAY_INTERVAL : DateTimeUtils.HOUR_INTERVAL;
        TreeMap<Long, String> writePartitionCollector = getWritePartitions(writeResults);
        Map.Entry<Long, String> minPartition = writePartitionCollector.firstEntry();
        Map.Entry<Long, String> maxPartition = writePartitionCollector.lastEntry();
        long archerCommitStartTime = partitionState.lastArcherCommitTime;
        long archerCommitEndTime = maxPartition.getKey();
        if (archerCommitStartTime == 0) {
          archerCommitStartTime = minPartition.getKey();
        }

        if (archerCommitEndTime > watermark && watermark != Long.MIN_VALUE) {
          archerCommitEndTime = watermark;
        }
        archerCommitEndTime = waterMarkToCurrentPartitionTime(archerCommitEndTime - partitionInterval, archerCommitType);
        List<List<String>> needCommitArcher = getNeedCommitList(archerCommitStartTime, archerCommitEndTime, partitionInterval, archerCommitType);

        boolean commitToArcherResult = commitToArcher(database, table, archerPartitionField, needCommitArcher);
        if (commitToArcherResult) {
          partitionState.lastArcherCommitTime = archerCommitEndTime + partitionInterval;
        }
        LOG.info("commit to archer successful ,lastArcherCommitTime: {} , watermark: {}", partitionState.lastArcherCommitTime, watermark);

      } catch (Exception e) {
        LOG.error("commit to archer error", e);
      }
    }
  }

  public boolean commitToArcher(String database, String table, List<String> partitionKeys, List<List<String>> needCommit) throws Exception {
    for (List<String> partitionValue : needCommit) {
      boolean updateResult = this.archerOperator.updatePartitionStatus(database, table, partitionKeys, partitionValue);
      if (!updateResult) {
        LOG.error("archer commit failed,will try again later");
        return false;
      }
    }
    return true;
  }

}
