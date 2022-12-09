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

package org.apache.hudi.keygen;

import org.apache.avro.generic.GenericRecord;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class CustomRecordKeyGenerator extends BaseKeyGenerator {
  private static final Logger LOG = LoggerFactory.getLogger(CustomRecordKeyGenerator.class);

  protected List<String> recordKeyFields;
  protected List<String> partitionPathFields;
  protected final boolean hiveStylePartition;
  private final SimpleDateFormat sourceTimeFormat;
  private final SimpleDateFormat targetDateTimeFormat;
  private final SimpleDateFormat targetHourTimeFormat;

  public enum PartitionKeyType {
    SIMPLE, FIX, LOG_DATE, LOG_HOUR, TIMESTAMP_10, TIMESTAMP_13
  }

  public CustomRecordKeyGenerator(TypedProperties props) {
    this(props, props.getString(KeyGeneratorOptions.RECORDKEY_FIELD_OPT_KEY),
        props.getString(KeyGeneratorOptions.PARTITIONPATH_FIELD_OPT_KEY));
  }

  CustomRecordKeyGenerator(TypedProperties props, String recordKeyField, String partitionPathField) {
    super(props);

    this.recordKeyFields = Arrays.stream(recordKeyField.split(","))
        .map(String::trim).collect(Collectors.toList());
    this.partitionPathFields = Arrays.stream(partitionPathField.split(","))
        .map(String::trim).collect(Collectors.toList());
    this.hiveStylePartition = Boolean.parseBoolean(props.getString(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key()));
    this.sourceTimeFormat = new SimpleDateFormat(CustomKeyGenUtils.DEFAULT_TIME_SOURCE_FORMAT);
    this.targetDateTimeFormat = new SimpleDateFormat(CustomKeyGenUtils.DEFAULT_TIME_TARGET_DATE_FORMAT);
    this.targetHourTimeFormat = new SimpleDateFormat(CustomKeyGenUtils.DEFAULT_TIME_TARGET_HOUR_FORMAT);
  }

  @Override
  public String getRecordKey(GenericRecord record) {
    if (getRecordKeyFields().isEmpty()) {
      throw new HoodieException("Unable to find field names for record key in cfg");
    }
    return CustomKeyGenUtils.getRecordKey(record, getRecordKeyFields());
  }

  @Override
  public String getPartitionPath(GenericRecord record) {
    if (getPartitionPathFields().size() == 1 && getPartitionPathFields().get(0).isEmpty()) {
      return "";
    }
    StringBuilder partitionPath = new StringBuilder();
    for (String field : getPartitionPathFields()) {
      String[] filedWithType = field.split(CustomKeyGenUtils.SPLIT_REGEX);
      if (filedWithType.length != 3) {
        throw new HoodieException("Unable to find field names for partition path in proper format, field = " + field);
      }

      String partitionPathField = filedWithType[0];
      PartitionKeyType keyType = PartitionKeyType.valueOf(filedWithType[1].toUpperCase());
      String partitionPreFix = filedWithType[2];
      switch (keyType) {
        case FIX:
          if (hiveStylePartition) {
            partitionPath.append(partitionPreFix).append("=");
          }
          partitionPath.append(partitionPathField);
          break;
        case SIMPLE:
          partitionPath.append(CustomKeyGenUtils.getPartitionPath(record, partitionPathField, partitionPreFix, hiveStylePartition));
          break;
        case LOG_DATE:
          partitionPath.append(
              CustomKeyGenUtils.getTimestampPartitionPath(record, partitionPathField, partitionPreFix,
                  hiveStylePartition, sourceTimeFormat, targetDateTimeFormat));
          break;
        case LOG_HOUR:
          partitionPath.append(
              CustomKeyGenUtils.getTimestampPartitionPath(record, partitionPathField, partitionPreFix,
                  hiveStylePartition, sourceTimeFormat, targetHourTimeFormat));
          break;
        case TIMESTAMP_10:
          partitionPath.append(
              CustomKeyGenUtils.getTimestampPartitionPath(record, partitionPathField, partitionPreFix,
                  hiveStylePartition, targetDateTimeFormat, true));
          break;
        case TIMESTAMP_13:
          partitionPath.append(
              CustomKeyGenUtils.getTimestampPartitionPath(record, partitionPathField, partitionPreFix,
                  hiveStylePartition, targetDateTimeFormat, false));
          break;
        default:
          throw new HoodieException("Please provide valid PartitionKeyType with fields! You provided: " + keyType);
      }
      partitionPath.append(CustomKeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);
    return partitionPath.toString();
  }

  public List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  public List<String> getPartitionPathFields() {
    return partitionPathFields;
  }
}
