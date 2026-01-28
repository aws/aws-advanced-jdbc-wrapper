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

package software.amazon.jdbc.plugin.gdbfailover;

import java.util.HashMap;
import java.util.Map;
import org.checkerframework.checker.nullness.qual.Nullable;

public enum GlobalDbFailoverMode {
  STRICT_WRITER,
  STRICT_HOME_READER,
  STRICT_OUT_OF_HOME_READER,
  STRICT_ANY_READER,
  HOME_READER_OR_WRITER,
  OUT_OF_HOME_READER_OR_WRITER,
  ANY_READER_OR_WRITER;

  private static final Map<String, GlobalDbFailoverMode> nameToValue = new HashMap<String, GlobalDbFailoverMode>() {
    {
      put("strict-writer", STRICT_WRITER);
      put("strict-home-reader", STRICT_HOME_READER);
      put("strict-out-of-home-reader", STRICT_OUT_OF_HOME_READER);
      put("strict-any-reader", STRICT_ANY_READER);
      put("home-reader-or-writer", HOME_READER_OR_WRITER);
      put("out-of-home-reader-or-writer", OUT_OF_HOME_READER_OR_WRITER);
      put("any-reader-or-writer", ANY_READER_OR_WRITER);
    }
  };

  public static @Nullable GlobalDbFailoverMode fromValue(final @Nullable String value) {
    if (value == null) {
      return null;
    }
    GlobalDbFailoverMode mode = nameToValue.get(value.toLowerCase());
    if (mode == null) {
      throw new IllegalArgumentException("Invalid OutOfHomeFailoverMode value: " + value);
    }
    return mode;
  }
}
