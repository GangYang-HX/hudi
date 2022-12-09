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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.keygen.constant.KeyGeneratorOptions;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.sink.bulk.RowDataKeyGenInterface;
import org.apache.hudi.util.RowDataProjection;

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.List;

public class CustomRowDataKeyGenerator implements RowDataKeyGenInterface, Serializable {

  protected String[] recordKeyFields;
  protected String[] partitionPathFields;
  protected final boolean hiveStylePartition;
  private final SimpleDateFormat sourceTimeFormat;
  private final SimpleDateFormat targetDateTimeFormat;
  private final SimpleDateFormat targetHourTimeFormat;
  private final RowDataProjection recordKeyProjection;
  private final RowDataProjection partitionPathProjection;

  public CustomRowDataKeyGenerator(TypedProperties props, RowType rowType) {
    this(props, props.getString(FlinkOptions.RECORD_KEY_FIELD.key()),
        props.getString(FlinkOptions.PARTITION_PATH_FIELD.key()),rowType);
  }

  CustomRowDataKeyGenerator(TypedProperties props, String recordKeyField, String partitionPathField,RowType rowType) {
    this.recordKeyFields = recordKeyField.split(",");
    this.partitionPathFields = partitionPathField.split(",");
    this.hiveStylePartition = Boolean.parseBoolean(props.getString(KeyGeneratorOptions.HIVE_STYLE_PARTITIONING_ENABLE.key()));
    this.sourceTimeFormat = new SimpleDateFormat(CustomKeyGenUtils.DEFAULT_TIME_SOURCE_FORMAT);
    this.targetDateTimeFormat = new SimpleDateFormat(CustomKeyGenUtils.DEFAULT_TIME_TARGET_DATE_FORMAT);
    this.targetHourTimeFormat = new SimpleDateFormat(CustomKeyGenUtils.DEFAULT_TIME_TARGET_HOUR_FORMAT);
    this.recordKeyProjection = RowDataKeyGen.getProjection(recordKeyFields, rowType.getFieldNames(), rowType.getChildren());
    this.partitionPathProjection = getPartitionProjection(partitionPathFields, rowType.getFieldNames(), rowType.getChildren());
  }

  public RowDataProjection getPartitionProjection(String[] fields, List<String> schemaFields, List<LogicalType> schemaTypes) {
    String [] mappingFiled = new String[fields.length];
    for (int i = 0;i < fields.length; i++) {
      String field = partitionPathFields[i];
      String[] filedWithType = field.split(CustomKeyGenUtils.SPLIT_REGEX);
      if (filedWithType.length != 3) {
        throw new HoodieException("Unable to find field names for partition path in proper format, field = " + field);
      }
      mappingFiled[i] = filedWithType[0];
    }
    return RowDataKeyGen.getProjection(mappingFiled, schemaFields, schemaTypes);
  }

  @Override
  public String getRecordKey(RowData rowData) {
    Object[] keyValues = this.recordKeyProjection.projectAsValues(rowData);
    return RowDataKeyGen.getRecordKey(keyValues, this.recordKeyFields);
  }

  @Override
  public String getPartitionPath(RowData rowData) {
    if (partitionPathFields == null || partitionPathFields.length == 0) {
      return "";
    }
    Object[] partitionPathValues = partitionPathProjection.projectAsValues(rowData);
    StringBuilder partitionPath = new StringBuilder();
    for (int i = 0; i < partitionPathFields.length; i++) {
      String field = partitionPathFields[i];
      String[] filedWithType = field.split(CustomKeyGenUtils.SPLIT_REGEX);
      if (filedWithType.length != 3) {
        throw new HoodieException("Unable to find field names for partition path in proper format, field = " + field);
      }

      String partitionPathField = filedWithType[0];
      CustomRecordKeyGenerator.PartitionKeyType keyType = CustomRecordKeyGenerator.PartitionKeyType.valueOf(filedWithType[1].toUpperCase());
      String partitionPreFix = filedWithType[2];
      switch (keyType) {
        case FIX:
          if (hiveStylePartition) {
            partitionPath.append(partitionPreFix).append("=");
          }
          partitionPath.append(partitionPathField);
          break;
        case SIMPLE:
          partitionPath.append(CustomKeyGenUtils.getPartitionPathByValue(partitionPathValues[i].toString(), partitionPathField, partitionPreFix, hiveStylePartition));
          break;
        case LOG_DATE:
          partitionPath.append(
              CustomKeyGenUtils.getTimestampPartitionPathByObject(partitionPathValues[i], partitionPathField, partitionPreFix,
                  hiveStylePartition, sourceTimeFormat, targetDateTimeFormat));
          break;
        case LOG_HOUR:
          partitionPath.append(
              CustomKeyGenUtils.getTimestampPartitionPathByObject(partitionPathValues[i], partitionPathField, partitionPreFix,
                  hiveStylePartition, sourceTimeFormat, targetHourTimeFormat));
          break;
        default:
          throw new HoodieException("Please provide valid PartitionKeyType with fields! You provided: " + keyType);
      }
      partitionPath.append(CustomKeyGenUtils.DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    return partitionPath.toString();
  }
}
