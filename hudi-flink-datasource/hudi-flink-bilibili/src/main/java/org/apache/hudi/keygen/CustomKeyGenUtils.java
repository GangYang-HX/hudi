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

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.exception.HoodieException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class CustomKeyGenUtils {
  private static final String NULL_RECORD_KEY_PLACEHOLDER = "__null__";
  private static final String EMPTY_RECORD_KEY_PLACEHOLDER = "__empty__";

  public static final String DEFAULT_SIMPLE_PARTITION_PATH = "all";
  private static final String DEFAULT_TIME_PARTITION_PATH = "2000-01-01 00:00:00";
  public static final String DEFAULT_TIME_SOURCE_FORMAT = "yyyy-MM-dd HH:mm:ss";
  public static final String DEFAULT_TIME_TARGET_DATE_FORMAT = "yyyyMMdd";
  public static final String DEFAULT_TIME_TARGET_HOUR_FORMAT = "HH";
  public static final String SPLIT_REGEX = ":";
  public static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

  public static String getRecordKey(GenericRecord record, List<String> recordKeyFields) {
    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (String recordKeyField : recordKeyFields) {
      String recordKeyValue = HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyField, true, false);
      if (recordKeyValue == null) {
        recordKey.append(recordKeyField).append(":").append(NULL_RECORD_KEY_PLACEHOLDER).append(";");
      } else if (recordKeyValue.isEmpty()) {
        recordKey.append(recordKeyField).append(":").append(EMPTY_RECORD_KEY_PLACEHOLDER).append(";");
      } else {
        recordKey.append(recordKeyField).append(":").append(recordKeyValue).append(";");
        keyIsNullEmpty = false;
      }
    }
    recordKey.deleteCharAt(recordKey.length() - 1);
    if (keyIsNullEmpty) {
      throw new HoodieException("recordKey values: \"" + recordKey + "\" for fields: "
          + recordKeyFields.toString() + " cannot be entirely null or empty.");
    }
    return recordKey.toString();
  }

  public static String getPartitionPath(GenericRecord record, String partitionPathField,
                                        String partitionPreFix, boolean hiveStylePartition) {
    String partitionPath = HoodieAvroUtils.getNestedFieldValAsString(record, partitionPathField, true, false);
    if (partitionPath == null || partitionPath.isEmpty()) {
      partitionPath = DEFAULT_SIMPLE_PARTITION_PATH;
    }
    if (hiveStylePartition) {
      partitionPath = partitionPreFix + "=" + partitionPath;
    }
    return partitionPath;
  }

  public static String getPartitionPathByValue(String partitionPath, String partitionPathField,
                                               String partitionPreFix, boolean hiveStylePartition) {
    if (partitionPath == null || partitionPath.isEmpty()) {
      partitionPath = DEFAULT_SIMPLE_PARTITION_PATH;
    }
    if (hiveStylePartition) {
      partitionPath = partitionPreFix + "=" + partitionPath;
    }
    return partitionPath;
  }

  public static String getTimestampPartitionPath(GenericRecord record, String partitionPathField, String partitionPreFix,
                                                 boolean hiveStylePartition, SimpleDateFormat sourceDateFormat, SimpleDateFormat targetDataFormat) {
    Object partitionVal = HoodieAvroUtils.getNestedFieldVal(record, partitionPathField, true, false);
    return getTimestampPartitionPathByObject(partitionVal,partitionPathField, partitionPreFix,
        hiveStylePartition, sourceDateFormat, targetDataFormat);
  }

  public static String getTimestampPartitionPathByObject(Object partitionVal, String partitionPathField, String partitionPreFix,
                                                        boolean hiveStylePartition, SimpleDateFormat sourceDateFormat, SimpleDateFormat targetDataFormat) {
    if (partitionVal == null || partitionVal.toString().isEmpty()) {
      partitionVal = DEFAULT_TIME_PARTITION_PATH;
    }
    try {
      String partitionPath;
      partitionPath = targetDataFormat.format(sourceDateFormat.parse(String.valueOf(partitionVal)));
      if (hiveStylePartition) {
        partitionPath = partitionPreFix + "=" + partitionPath;
      }
      return partitionPath;
    } catch (Exception e) {
      throw new HoodieException(
          "Time stamp format exception with time in record:" + partitionVal
              + " field:{" + partitionPathField + "} value:{" + partitionVal + "}", e);
    }
  }

  public static String getTimestampPartitionPath(GenericRecord record, String partitionPathField, String partitionPreFix,
                                                 boolean hiveStylePartition, SimpleDateFormat targetDataFormat, boolean isUnix) {
    Object partitionVal = HoodieAvroUtils.getNestedFieldVal(record, partitionPathField, true, false);
    if (partitionVal == null || partitionVal.toString().isEmpty()) {
      partitionVal = 0L;
    }
    try {
      long ts = Long.parseLong(String.valueOf(partitionVal));
      if (isUnix) {
        ts = ts * 1000L;
      }
      String partitionPath = targetDataFormat.format(new Date(ts));
      if (hiveStylePartition) {
        partitionPath = partitionPreFix + "=" + partitionPath;
      }
      return partitionPath;
    } catch (Exception e) {
      throw new HoodieException(
          "Time stamp format exception with time in record:" + record.toString()
              + " field:{" + partitionPathField + "} value:{" + partitionVal + "}", e);
    }
  }
}
