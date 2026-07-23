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

package software.amazon.jdbc.plugin.readwritesplitting.updater;

import java.sql.Connection;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;
import software.amazon.jdbc.plugin.readwritesplitting.classifier.RoleClassifier;

/** Topology {@link ConnectionUpdatePolicy}: adopt based purely on the current host's role. */
public class RoleBasedUpdatePolicy implements ConnectionUpdatePolicy {

  private final RoleClassifier roleClassifier;

  public RoleBasedUpdatePolicy(final RoleClassifier roleClassifier) {
    this.roleClassifier = roleClassifier;
  }

  @Override
  public boolean shouldUpdateWriter(
      final RwSplitContext ctx, final Connection currentConnection, final HostSpec currentHost) {
    return this.roleClassifier.isWriter(currentHost);
  }

  @Override
  public boolean shouldUpdateReader(
      final RwSplitContext ctx, final Connection currentConnection, final HostSpec currentHost) {
    return this.roleClassifier.isReader(currentHost);
  }
}
