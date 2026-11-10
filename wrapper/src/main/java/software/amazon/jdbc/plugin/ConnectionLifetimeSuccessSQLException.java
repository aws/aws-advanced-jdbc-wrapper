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

package software.amazon.jdbc.plugin;

import software.amazon.jdbc.util.Messages;
import software.amazon.jdbc.util.SqlState;

public class ConnectionLifetimeSuccessSQLException extends ConnectionLifetimeSQLException {

  public ConnectionLifetimeSuccessSQLException(final Throwable cause) {
    super(
        Messages.get("ConnectionLifetimeSuccessSQLException.connectionChanged"),
        SqlState.COMMUNICATION_LINK_CHANGED.getState(),
        cause);
  }

  public ConnectionLifetimeSuccessSQLException() {
    super(
        Messages.get("ConnectionLifetimeSuccessSQLException.connectionChanged"),
        SqlState.COMMUNICATION_LINK_CHANGED.getState());
  }
}
