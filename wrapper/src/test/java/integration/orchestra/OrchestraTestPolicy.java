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

package integration.orchestra;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

/** Shared selection policy for Orchestra test compositions. */
public final class OrchestraTestPolicy {

  private OrchestraTestPolicy() {
  }

  /** Returns whether a matrix should continue after one composition fails. */
  public static boolean continueAfterFailure(final int engineCount, final int jvmCount) {
    return engineCount * jvmCount > 1;
  }

  /** Returns each authorized region once, with the primary first. */
  public static List<String> authorizedRegions(
      final String primary, final List<String> secondaryRegions) {
    final LinkedHashSet<String> regions = new LinkedHashSet<>();
    regions.add(primary);
    if (secondaryRegions != null) {
      regions.addAll(secondaryRegions);
    }
    return new ArrayList<>(regions);
  }

  /** Appends a tag without replacing suite or command-line selections already present. */
  public static void appendTag(
      final Map<String, String> properties, final String property, final String tag) {
    final String existing = properties.get(property);
    properties.put(property,
        existing == null || existing.trim().isEmpty() ? tag : existing + "," + tag);
  }
}
