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

package org.apache.hudi.sink.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class DateTimeUtils {

  public static final String YYYYMMDD = "yyyyMMdd";
  public static final String YYYYMMDDHH = "yyyyMMddHH";
  public static final String YYYYMMDD_HH = "yyyyMMdd_HH";
  public static final String LOG_DATE = "log_date";
  public static final String LOG_HOUR = "log_hour";
  public static final long HOUR_INTERVAL = 1 * 60 * 60 * 1000;
  public static final long DAY_INTERVAL = 24 * 60 * 60 * 1000;

  public static String time2String(long time, String pattern) {
    return date2String(new Date(time), pattern);
  }

  public static String date2String(Date date, String pattern) {
    if (date == null || pattern == null) {
      return null;
    }
    return new SimpleDateFormat(pattern).format(date);
  }

  public static Long string2Time(String dateStr, String pattern) throws ParseException {
    if (dateStr == null || pattern == null) {
      return null;
    }
    return new SimpleDateFormat(pattern).parse(dateStr).getTime();
  }

}
