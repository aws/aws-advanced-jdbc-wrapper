/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

package software.aws.rds.jdbc.proxydriver.util;

import org.checkerframework.checker.nullness.qual.Nullable;

public class SqlState {

  public static final SqlState UNKNOWN_STATE = new SqlState("");

  // TODO: add custom error codes support

  private final String sqlState;

  SqlState(String sqlState) {
    this.sqlState = sqlState;
  }

  public String getCode() {
    return this.sqlState;
  }

  public static boolean isConnectionError(@Nullable String psqlState) {
    // TODO
    return false;
  }
}
