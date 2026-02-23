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

/**
 * Supported encryption algorithms for the KMS encryption plugin.
 */
public enum EncryptionAlgorithm {
  /** AES-256 with Galois/Counter Mode. */
  AES_256_GCM("AES-256-GCM", 32),
  
  /** AES-128 with Galois/Counter Mode. */
  AES_128_GCM("AES-128-GCM", 16);

  private final String algorithmName;
  private final int keyLength;

  EncryptionAlgorithm(String algorithmName, int keyLength) {
    this.algorithmName = algorithmName;
    this.keyLength = keyLength;
  }

  /**
   * Returns the algorithm name.
   *
   * @return the algorithm name
   */
  public String getAlgorithmName() {
    return algorithmName;
  }

  /**
   * Returns the required key length in bytes.
   *
   * @return the key length
   */
  public int getKeyLength() {
    return keyLength;
  }

  /**
   * Finds an algorithm by name.
   *
   * @param algorithmName the algorithm name
   * @return the matching algorithm, or null if not found
   */
  public static EncryptionAlgorithm fromString(String algorithmName) {
    if (algorithmName == null) {
      return null;
    }
    for (EncryptionAlgorithm algorithm : values()) {
      if (algorithm.algorithmName.equals(algorithmName)) {
        return algorithm;
      }
    }
    return null;
  }

  /**
   * Checks if an algorithm name is supported.
   *
   * @param algorithmName the algorithm name to check
   * @return true if supported, false otherwise
   */
  public static boolean isSupported(String algorithmName) {
    return fromString(algorithmName) != null;
  }

  @Override
  public String toString() {
    return algorithmName;
  }
}
