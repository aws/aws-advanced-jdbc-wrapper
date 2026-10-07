/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package software.amazon.jdbc.util;

public class DriverImportantEventService {

  private static final ImportantEventService INSTANCE = new ImportantEventService(
      ImportantEventService.isEnabledByDefault(),
      Long.parseLong(
          System.getProperty(
              ImportantEventService.DRIVER_QUEUE_TTL_PROPERTY,
              String.valueOf(ImportantEventService.DEFAULT_QUEUE_TTL_MS))),
      Integer.parseInt(
          System.getProperty(
              ImportantEventService.DRIVER_QUEUE_MAX_SIZE_PROPERTY,
              String.valueOf(ImportantEventService.DEFAULT_QUEUE_MAX_SIZE))),
      ImportantEventService.defaultMethodHistorySize());

  private DriverImportantEventService() {}

  public static ImportantEventService getInstance() {
    return INSTANCE;
  }

  public static void clear() {
    INSTANCE.clear();
  }
}
