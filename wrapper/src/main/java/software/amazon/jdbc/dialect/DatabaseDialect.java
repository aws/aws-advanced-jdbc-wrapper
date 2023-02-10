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

package software.amazon.jdbc.dialect;

import software.amazon.jdbc.AwsWrapperProperty;
import software.amazon.jdbc.exceptions.ExceptionHandler;

public interface DatabaseDialect {
  public static final AwsWrapperProperty DATABASE_DIALECT =
      new AwsWrapperProperty(
          "databaseDialect",
          "false",
          "Set to true to automatically load-balance read-only transactions when setReadOnly is "
              + "set to true");

  public static DatabaseDialect getInstance() {
    return new DefaultDatabaseDialect();
  }

  public String getInstanceNameQuery();

  public String getInstanceNameColumn();

  public boolean isSupported();

  public int getDefaultPort();

  public ExceptionHandler getExceptionHandler();

  public String getTopologyQuery();

  public String getReadOnlyQuery();

  public String getReadOnlyColumnName();

  public String getHostPortQuery();

  public String getURLScheme();
}
