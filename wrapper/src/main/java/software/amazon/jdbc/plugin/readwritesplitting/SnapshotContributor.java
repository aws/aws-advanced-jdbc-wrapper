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

package software.amazon.jdbc.plugin.readwritesplitting;

import java.util.List;
import software.amazon.jdbc.util.Pair;

/**
 * Implemented by stateful read/write splitting helpers that contribute a fragment to the plugin's
 * diagnostic state snapshot. The unified plugin aggregates the fragments of all helpers that
 * implement this interface alongside its own shared state.
 *
 * <p>Fragments should use distinct key names to avoid collisions when helpers are combined.
 */
public interface SnapshotContributor {

  /**
   * Returns this helper's snapshot fragment as a list of key/value pairs. May be empty.
   *
   * @return the snapshot fragment, never {@code null}
   */
  List<Pair<String, Object>> snapshotState();
}
