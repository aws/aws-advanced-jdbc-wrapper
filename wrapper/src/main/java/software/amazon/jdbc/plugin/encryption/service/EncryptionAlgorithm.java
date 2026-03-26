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

package software.amazon.jdbc.plugin.encryption.service;

import java.util.HashMap;
import java.util.Map;
import software.amazon.jdbc.util.Messages;

/**
 * Supported encryption algorithms for the KMS encryption plugin.
 */
public final class EncryptionAlgorithm {

  public static final String AES_256_GCM = "AES-256-GCM";
  public static final String AES_128_GCM = "AES-128-GCM";

  private static final Map<String, Integer> KEY_LENGTHS = new HashMap<>();

  static {
    KEY_LENGTHS.put(AES_256_GCM, 32);
    KEY_LENGTHS.put(AES_128_GCM, 16);
  }

  private EncryptionAlgorithm() {
    // utility class
  }

  /**
   * Returns the required key length in bytes for the given algorithm.
   *
   * @param algorithmName the algorithm name
   * @return the key length in bytes
   * @throws IllegalArgumentException if the algorithm is not supported
   */
  public static int getKeyLength(String algorithmName) {
    Integer length = KEY_LENGTHS.get(algorithmName);
    if (length == null) {
      throw new IllegalArgumentException(
          Messages.get("EncryptionAlgorithm.unknownAlgorithm", new Object[]{algorithmName}));
    }
    return length;
  }

  /**
   * Checks if an algorithm name is supported.
   *
   * @param algorithmName the algorithm name to check
   * @return true if supported, false otherwise
   */
  public static boolean isSupported(String algorithmName) {
    return KEY_LENGTHS.containsKey(algorithmName);
  }
}
