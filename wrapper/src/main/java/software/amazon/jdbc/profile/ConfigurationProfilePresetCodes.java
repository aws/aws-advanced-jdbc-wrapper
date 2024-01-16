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

package software.amazon.jdbc.profile;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;

public class ConfigurationProfilePresetCodes {

  // Presets family A, B, C - no connection pool
  // Presets family D, E ,F - internal connection pool
  // Presets family G, H, I - external connection pool

  // Presets family SF_D, SF_E ,SF_F - internal connection pool; optimized for Spring Framework / Spring Boot

  public static final String A0 = "A0"; // Normal
  public static final String A1 = "A1"; // Easy
  public static final String A2 = "A2"; // Aggressive
  public static final String B = "B"; // Normal

  public static final String C0 = "C0"; // Normal
  public static final String C1 = "C1"; // Aggressive

  public static final String D0 = "D0"; // Normal
  public static final String D1 = "D1"; // Easy

  public static final String E = "E"; // Normal

  public static final String F0 = "F0"; // Normal
  public static final String F1 = "F1"; // Aggressive

  public static final String G0 = "G0"; // Normal
  public static final String G1 = "G1"; // Easy

  public static final String H = "H"; // Normal

  public static final String I0 = "I0"; // Normal
  public static final String I1 = "I1"; // Aggressive

  public static final String SF_D0 = "SF_D0"; // Normal
  public static final String SF_D1 = "SF_D1"; // Easy

  public static final String SF_E = "SF_E"; // Normal

  public static final String SF_F0 = "SF_F0"; // Normal
  public static final String SF_F1 = "SF_F1"; // Aggressive

  private static final Set<String> KNOWN_PRESETS = ConcurrentHashMap.newKeySet();

  static {
    registerProperties(String.class);
  }

  public static boolean isKnownPreset(final @NonNull String presetName) {
    return KNOWN_PRESETS.contains(presetName);
  }

  private static void registerProperties(final Class<?> ownerClass) {
    Arrays.stream(ownerClass.getDeclaredFields())
        .filter(
            f ->
                f.getType() == ownerClass
                    && Modifier.isPublic(f.getModifiers())
                    && Modifier.isStatic(f.getModifiers()))
        .forEach(f -> KNOWN_PRESETS.add(f.getName()));
  }
}
