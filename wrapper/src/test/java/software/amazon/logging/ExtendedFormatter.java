/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 */

package software.amazon.logging;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.SimpleFormatter;

public class ExtendedFormatter extends SimpleFormatter {

  /**
   * Format string arguments are presented below.
   * - ( 1$) the timestamp
   * - ( 2$) the source
   * - ( 3$) the logger name
   * - ( 4$) the log level
   * - ( 5$) the log message
   * - ( 6$) the throwable and its backtrace, if any
   * - ( 7$) the thread name
   * - ( 8$) the thread ID
   */
  private static final String format = LogManager.getLogManager()
      .getProperty(ExtendedFormatter.class.getName() + ".format");
  private final Date dat = new Date();
  private static final int THREAD_NAME_CACHE_SIZE = 10000;
  private static final String UNKNOWN_THREAD_NAME = "Unknown-";
  private static final Object threadMxBeanLock = new Object();
  private static volatile ThreadMXBean threadMxBean = null;

  private static final ThreadLocal<HashMap<Integer, String>> threadNameCache =
      ThreadLocal.withInitial(() -> new HashMap<>(THREAD_NAME_CACHE_SIZE));

  @Override
  public synchronized String format(LogRecord record) {
    dat.setTime(record.getMillis());
    String source;
    if (record.getSourceClassName() != null) {
      source = record.getSourceClassName();
      if (record.getSourceMethodName() != null) {
        source += " " + record.getSourceMethodName();
      }
    } else {
      source = record.getLoggerName();
    }
    String message = formatMessage(record);
    String throwable = "";
    if (record.getThrown() != null) {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      pw.println();
      record.getThrown().printStackTrace(pw);
      pw.close();
      throwable = sw.toString();
    }

    String threadName;
    if (Thread.currentThread().getId() == record.getThreadID()
        && record.getThreadID() < (Integer.MAX_VALUE / 2)) {
      threadName = Thread.currentThread().getName();
    } else {
      threadName = getThreadName(record.getThreadID());
    }

    return String.format(format,
        dat,
        source,
        record.getLoggerName(),
        record.getLevel().getLocalizedName(),
        message,
        throwable,
        threadName,
        record.getThreadID());
  }

  private static String getThreadName(int logRecordThreadId) {
    Map<Integer, String> cache = threadNameCache.get();
    String result = null;

    if (logRecordThreadId > (Integer.MAX_VALUE / 2)) {
      result = cache.get(logRecordThreadId);
    }

    if (result != null) {
      return result;
    }

    if (logRecordThreadId >= (Integer.MAX_VALUE / 2)) {
      result = UNKNOWN_THREAD_NAME + logRecordThreadId;
    } else {
      // Double checked locking OK as threadMxBean is volatile
      if (threadMxBean == null) {
        synchronized (threadMxBeanLock) {
          if (threadMxBean == null) {
            threadMxBean = ManagementFactory.getThreadMXBean();
          }
        }
      }
      ThreadInfo threadInfo = threadMxBean.getThreadInfo(logRecordThreadId);
      if (threadInfo == null) {
        return Long.toString(logRecordThreadId);
      }
      result = threadInfo.getThreadName();
    }

    cache.put(logRecordThreadId, result);

    return result;
  }
}
