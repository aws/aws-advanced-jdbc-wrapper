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

package software.amazon.jdbc.integration.orchestra;

import java.util.Collection;
import java.util.Collections;
import java.util.Set;

/** Pure suite-tag selection used by the in-container shard planner. */
public final class ShardSelection {

  private ShardSelection() {
  }

  public static boolean classSelected(
      final Set<String> classTags,
      final Collection<Set<String>> methodTags,
      final Set<String> includedTags,
      final Set<String> excludedTags) {

    if (!Collections.disjoint(classTags, excludedTags)) {
      return false;
    }
    if (includedTags.isEmpty() || !Collections.disjoint(classTags, includedTags)) {
      return true;
    }
    for (final Set<String> tags : methodTags) {
      if (Collections.disjoint(tags, excludedTags)
          && !Collections.disjoint(tags, includedTags)) {
        return true;
      }
    }
    return false;
  }
}
