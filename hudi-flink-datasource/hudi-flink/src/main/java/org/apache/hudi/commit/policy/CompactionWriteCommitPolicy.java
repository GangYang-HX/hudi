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

import org.apache.hudi.sink.compact.CompactionCommitEvent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * @author: chenshizhi
 * @time: 2022/7/11 1:14 下午
 */
public class CompactionWriteCommitPolicy extends DefaultWriteCommitPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionWriteCommitPolicy.class);

  private final Collection<CompactionCommitEvent> events;

  private List<CompactionCommitEvent> failedEvents;

  public CompactionWriteCommitPolicy(
      CommitHandler commitHandler,
      RollbackHandler rollbackHandler,
      BooleanSupplier errorParseSupplier,
      boolean shouldIgnoreError,
      String instant,
      Collection<CompactionCommitEvent> events) {
    super(
        commitHandler,
        rollbackHandler,
        errorParseSupplier,
        shouldIgnoreError,
        instant,
        events.stream()
            .filter(event -> !event.isFailed())
            .map(CompactionCommitEvent::getWriteStatuses)
            .flatMap(Collection::stream)
            .collect(Collectors.toList()));
    this.events = events;
  }

  @Override
  protected boolean errorStat() {
    this.failedEvents =
        events.stream().filter(CompactionCommitEvent::isFailed).collect(Collectors.toList());
    return super.errorStat() || !failedEvents.isEmpty();
  }

  @Override
  protected void metricDisplay() {
    if (!failedEvents.isEmpty()) {
      LOG.error(
          "Failed events when compact commit. Failed/Total=" + failedEvents.size() + "/" + events.size());
      LOG.error("The first 100 error compact commit events");
      failedEvents.stream()
          .limit(100)
          .forEach(
              commitEvent -> LOG.error(
                  "Commit failed for instant {}, fileID {} in task {}",
                  commitEvent.getInstant(),
                  commitEvent.getFileId(),
                  commitEvent.getTaskID()));
    }
    super.metricDisplay();
  }
}
