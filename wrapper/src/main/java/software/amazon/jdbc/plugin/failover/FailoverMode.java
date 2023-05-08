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

package software.amazon.jdbc.plugin.failover;

import java.util.HashMap;
import java.util.Map;

public enum FailoverMode {
  STRICT_WRITER,
  STRICT_READER,
  READER_OR_WRITER;

  private static final Map<String, FailoverMode> nameToValue = new HashMap<String, FailoverMode>() {
    {
      put("strict-writer", STRICT_WRITER);
      put("strict-reader", STRICT_READER);
      put("reader-or-writer", READER_OR_WRITER);
    }
  };

  public static FailoverMode fromValue(String value) {
    if (value == null) {
      return null;
    }
    return nameToValue.get(value.toLowerCase());
  }
}
