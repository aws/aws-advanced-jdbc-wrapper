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

import java.util.Arrays;
import java.util.List;

public class SubscribedMethodHelper {

  public static final List<String> NETWORK_BOUND_METHODS = Arrays.asList(
      "Connection.commit",
      "Connection.connect",
      "Connection.isValid",
      "Connection.rollback",
      "Connection.sendQueryCancel",
      "Connection.setAutoCommit",
      "Connection.setReadOnly",
      "Statement.cancel",
      "Statement.execute",
      "Statement.executeBatch",
      "Statement.executeLargeBatch",
      "Statement.executeLargeUpdate",
      "Statement.executeQuery",
      "Statement.executeUpdate",
      "Statement.executeWithFlags",
      "PreparedStatement.execute",
      "PreparedStatement.executeBatch",
      "PreparedStatement.executeLargeUpdate",
      "PreparedStatement.executeQuery",
      "PreparedStatement.executeUpdate",
      "PreparedStatement.executeWithFlags",
      "PreparedStatement.getParameterMetaData",
      "CallableStatement.execute",
      "CallableStatement.executeLargeUpdate",
      "CallableStatement.executeQuery",
      "CallableStatement.executeUpdate",
      "CallableStatement.executeWithFlags"
  );

  public static final List<String> METHODS_REQUIRING_UPDATED_TOPOLOGY = Arrays.asList(
      "Connection.commit",
      "Connection.connect",
      "Connection.isValid",
      "Connection.setAutoCommit",
      "Statement.execute",
      "Statement.executeBatch",
      "Statement.executeLargeBatch",
      "Statement.executeLargeUpdate",
      "Statement.executeQuery",
      "Statement.executeUpdate",
      "Statement.executeWithFlags",
      "PreparedStatement.execute",
      "PreparedStatement.executeBatch",
      "PreparedStatement.executeLargeUpdate",
      "PreparedStatement.executeQuery",
      "PreparedStatement.executeUpdate",
      "PreparedStatement.executeWithFlags",
      "PreparedStatement.getParameterMetaData",
      "CallableStatement.execute",
      "CallableStatement.executeLargeUpdate",
      "CallableStatement.executeQuery",
      "CallableStatement.executeUpdate",
      "CallableStatement.executeWithFlags"
  );
}
