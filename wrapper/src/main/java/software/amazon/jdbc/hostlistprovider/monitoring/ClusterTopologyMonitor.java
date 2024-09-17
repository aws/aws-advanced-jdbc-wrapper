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

package software.amazon.jdbc.hostlistprovider.monitoring;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.HostSpec;

public interface ClusterTopologyMonitor extends AutoCloseable, Runnable {

  boolean canDispose();

  void setClusterId(final String clusterId);

  List<HostSpec> forceRefresh(final boolean writerImportant, final long timeoutMs)
      throws SQLException, TimeoutException;

  List<HostSpec> forceRefresh(final @Nullable Connection connection, final long timeoutMs)
      throws SQLException, TimeoutException;
}
