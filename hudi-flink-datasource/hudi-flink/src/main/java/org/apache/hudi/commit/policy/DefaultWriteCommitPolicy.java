/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.commit.policy;

import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.List;
import java.util.function.BooleanSupplier;

/**
 * @author: chenshizhi
 * @time: 2022/7/7 7:25 下午
 */
public class DefaultWriteCommitPolicy implements WriteCommitPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultWriteCommitPolicy.class);

  private final WriteCommitPolicy.CommitHandler commitHandler;

  private final WriteCommitPolicy.RollbackHandler rollbackHandler;

  private BooleanSupplier errorParseSupplier;

  protected boolean hasErrors = false;

  private final boolean shouldIgnoreError;

  private final String instant;

  private final Long watermark;

  private final List<WriteStatus> writeResults;

  private long totalErrorRecords;

  private long totalRecords;

  public DefaultWriteCommitPolicy(
      WriteCommitPolicy.CommitHandler commitHandler,
      WriteCommitPolicy.RollbackHandler rollbackHandler,
      boolean shouldIgnoreError,
      String instant,
      Long watermark,
      @Nonnull List<WriteStatus> writeResults) {
    this.commitHandler = commitHandler;
    this.rollbackHandler = rollbackHandler;
    this.shouldIgnoreError = shouldIgnoreError;
    this.instant = instant;
    this.watermark = watermark;
    this.writeResults = Preconditions.checkNotNull(writeResults);
  }

  public DefaultWriteCommitPolicy(
      WriteCommitPolicy.CommitHandler commitHandler,
      WriteCommitPolicy.RollbackHandler rollbackHandler,
      BooleanSupplier errorParseSupplier,
      boolean shouldIgnoreError,
      String instant,
      @Nonnull List<WriteStatus> writeResults) {
    this.commitHandler = commitHandler;
    this.rollbackHandler = rollbackHandler;
    this.shouldIgnoreError = shouldIgnoreError;
    this.instant = instant;
    this.writeResults = Preconditions.checkNotNull(writeResults);
    this.errorParseSupplier = errorParseSupplier;
    this.watermark = null;
  }

  @Override
  public void initialize() {
    if (errorParseSupplier != null) {
      this.hasErrors = errorParseSupplier.getAsBoolean();
      if (hasErrors) {
        errorStat();
      }
    } else {
      this.hasErrors = errorStat();
    }
  }

  @Override
  public void handleCommitOrRollback() {
    // commit or rollback
    if (!hasErrors || shouldIgnoreError) {
      handleCommit();
    } else {
      handleRollback();
    }
  }

  protected void handleCommit() {
    if (hasErrors) {
      LOG.warn("Some records failed to merge but forcing commit since commitOnErrors set to true");
      metricDisplay();
    }
    // commit instant
    commitHandler.handle(instant, watermark, writeResults);
  }

  protected void handleRollback() {
    ValidationUtils.checkState(hasErrors, "Rollback happens only when hasErrors is true.");
    metricDisplay();
    // rollback instant
    rollbackHandler.handle(instant, writeResults);
  }

  protected boolean errorStat() {
    totalErrorRecords =
        writeResults.stream().map(WriteStatus::getTotalErrorRecords).reduce(Long::sum).orElse(0L);
    totalRecords =
        writeResults.stream().map(WriteStatus::getTotalRecords).reduce(Long::sum).orElse(0L);

    return totalErrorRecords > 0;
  }

  protected void metricDisplay() {
    if (totalErrorRecords > 0) {
      LOG.error("Error when writing. Errors/Total=" + totalErrorRecords + "/" + totalRecords);
      LOG.error("The first 100 error messages");
      writeResults.stream()
          .filter(WriteStatus::hasErrors)
          .limit(100)
          .forEach(
              ws -> {
                LOG.error(
                    "Global error for partition path {} and fileID {}: {}",
                    ws.getPartitionPath(),
                    ws.getFileId(),
                    ws.getGlobalError());
                if (ws.getErrors().size() > 0) {
                  ws.getErrors()
                      .forEach(
                          (key, value) ->
                              LOG.error("Error for key:" + key + " and value " + value));
                }
              });
    }
  }
}
