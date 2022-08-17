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

package software.aws.jdbc.wrapper;

import java.sql.SQLException;
import java.sql.Savepoint;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.aws.jdbc.ConnectionPluginManager;
import software.aws.jdbc.util.WrapperUtils;

public class SavepointWrapper implements Savepoint {

  protected Savepoint savepoint;
  protected ConnectionPluginManager pluginManager;

  public SavepointWrapper(
      @NonNull Savepoint savepoint, @NonNull ConnectionPluginManager pluginManager) {
    this.savepoint = savepoint;
    this.pluginManager = pluginManager;
  }

  @Override
  public int getSavepointId() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        int.class,
        SQLException.class,
        this.pluginManager,
        this.savepoint,
        "Savepoint.getSavepointId",
        () -> this.savepoint.getSavepointId());
  }

  @Override
  public String getSavepointName() throws SQLException {
    return WrapperUtils.executeWithPlugins(
        String.class,
        SQLException.class,
        this.pluginManager,
        this.savepoint,
        "Savepoint.getSavepointName",
        () -> this.savepoint.getSavepointName());
  }
}
