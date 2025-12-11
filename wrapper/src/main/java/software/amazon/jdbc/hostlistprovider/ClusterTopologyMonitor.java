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

package software.amazon.jdbc.hostlistprovider;

import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.TimeoutException;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.util.events.EventSubscriber;
import software.amazon.jdbc.util.monitoring.Monitor;

public interface ClusterTopologyMonitor extends Monitor, EventSubscriber {

  boolean canDispose();

  List<HostSpec> forceRefresh(final boolean writerImportant, final long timeoutMs)
      throws SQLException, TimeoutException;
}
