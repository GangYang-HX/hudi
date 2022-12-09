package org.apache.hudi.commit.archer.configuration;

import org.apache.hudi.commit.policy.ArcherAndHMSPartitionCommitPolicy;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.SetupOptions;
import org.apache.hudi.sink.CommitType;

import org.apache.flink.configuration.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public class BILISetupOptions implements SetupOptions {
  private static final Logger LOG = LoggerFactory.getLogger(BILISetupOptions.class);
  public void setupOptions(Configuration conf) {
    setupArcherOptions(conf);
  }

  private static void setupArcherOptions(Configuration conf) {
    String[] partitions = conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",");
    if (!partitions[0].isEmpty()) {
      conf.setBoolean(ArcherOptions.ARCHER_COMMIT_ENABLED, conf.getOptional(ArcherOptions.ARCHER_COMMIT_ENABLED).orElse(true));
    }
    // TODO split the HMS and Archer Policy
    if (conf.getBoolean(ArcherOptions.ARCHER_COMMIT_ENABLED) || conf.getBoolean(FlinkOptions.HIVE_SYNC_ENABLED)) {
      conf.setString(FlinkOptions.PARTITION_COMMIT_POLICY_CLASS_NAME, ArcherAndHMSPartitionCommitPolicy.class.getName());
    }
    if (conf.getBoolean(ArcherOptions.ARCHER_COMMIT_ENABLED)) {
      if (conf.getString(ArcherOptions.ARCHER_COMMIT_DB).equals(ArcherOptions.ARCHER_COMMIT_DB.defaultValue())
          && !conf.getString(FlinkOptions.HIVE_SYNC_DB).equals(FlinkOptions.HIVE_SYNC_DB.defaultValue())) {
        conf.setString(ArcherOptions.ARCHER_COMMIT_DB, conf.getString(FlinkOptions.HIVE_SYNC_DB));
      }

      if (conf.getString(ArcherOptions.ARCHER_COMMIT_TABLE).equals(ArcherOptions.ARCHER_COMMIT_TABLE.defaultValue())
          && !conf.getString(FlinkOptions.HIVE_SYNC_TABLE).equals(FlinkOptions.HIVE_SYNC_TABLE.defaultValue())) {
        conf.setString(ArcherOptions.ARCHER_COMMIT_TABLE, conf.getString(FlinkOptions.HIVE_SYNC_TABLE));
      }

      Set<String> partitionFields = Arrays.stream(conf.getString(FlinkOptions.PARTITION_PATH_FIELD).split(",")).collect(Collectors.toSet());
      for (String partitionKey : partitionFields) {
        LOG.info("partitionKeys is {}", partitionKey);
      }
      if (partitionFields.contains(CommitType.LOG_DATE.name().toLowerCase()) && partitionFields.contains(CommitType.LOG_HOUR.name().toLowerCase())) {
        conf.setString(ArcherOptions.ARCHER_COMMIT_TYPE, CommitType.LOG_HOUR.name());
      } else if (partitionFields.contains(CommitType.LOG_DATE.name().toLowerCase())) {
        conf.setString(ArcherOptions.ARCHER_COMMIT_TYPE, CommitType.LOG_DATE.name());
      }

      ValidationUtils.checkArgument(
          !(conf.getString(ArcherOptions.ARCHER_COMMIT_TYPE, "").toUpperCase().equals(CommitType.LOG_DATE)
              || conf.getString(ArcherOptions.ARCHER_COMMIT_TYPE, "").toUpperCase().equals(CommitType.LOG_HOUR)),
          String.format("%s must be log_date or log_hour", ArcherOptions.ARCHER_COMMIT_TYPE.key()));
    }
  }
}
