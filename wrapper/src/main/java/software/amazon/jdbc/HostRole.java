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

package software.amazon.jdbc;

import java.util.HashMap;
import java.util.Map;

public enum HostRole {
  UNKNOWN,
  WRITER,
  READER;

  private static final Map<String, HostRole> nameToVerifyConnectionTypeValue =
      // Does not map to UNKNOWN, as is not a valid verification type option.
      new HashMap<String, HostRole>() {
        {
          put("writer", WRITER);
          put("reader", READER);
        }
      };

  public static HostRole verifyConnectionTypeFromValue(String value) {
    if (value == null) {
      return null;
    }
    return nameToVerifyConnectionTypeValue.get(value.toLowerCase());
  }
}
