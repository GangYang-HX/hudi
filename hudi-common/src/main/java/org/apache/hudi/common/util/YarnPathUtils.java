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

package org.apache.hudi.common.util;

import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Random;

/**
 * @author wangfei
 * @create 2022-05-11 16:13
 */
public class YarnPathUtils {
  private static final Logger LOG = LoggerFactory.getLogger(YarnPathUtils.class);

  private static final String HUDI = "hudi";
  private static final String DEFAULT_PATH = "/tmp/";
  private static final String DEFAULT_YARN_PATH = "/mnt/storage00/";

  private static final String LOCAL_PATH = System.getenv(ApplicationConstants.Environment.LOCAL_DIRS.key());

  private static final Random RANDOM = new Random();

  public static String initTmpPathOnYarnPath() {
    String basePath = new File(DEFAULT_YARN_PATH).exists() ? DEFAULT_YARN_PATH : DEFAULT_PATH;

    LOG.info("Yarn local path is:{}", LOCAL_PATH);

    if (!StringUtils.isNullOrEmpty(LOCAL_PATH)) {
      String[] localPathArr = LOCAL_PATH.split(",");

      StringBuilder stringBuilder = new StringBuilder();
      stringBuilder.append(localPathArr[RANDOM.nextInt(localPathArr.length)]);
      stringBuilder.append("/");
      stringBuilder.append(HUDI);
      stringBuilder.append("/");

      basePath = stringBuilder.toString();
    }
    LOG.info("Now tmp base path is:{}", basePath);
    return basePath;
  }
}
