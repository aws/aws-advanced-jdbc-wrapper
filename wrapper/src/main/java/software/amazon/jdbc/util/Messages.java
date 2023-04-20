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

package software.amazon.jdbc.util;

import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import java.text.MessageFormat;
import java.util.ResourceBundle;

public class Messages {

  private static final ResourceBundle MESSAGES = ResourceBundle.getBundle("messages");
  public static final Object[] emptyArgs = {};

  /**
   * Retrieve the localized error message associated with the provided key.
   *
   * @param key The key mapped to an error message.
   * @return The associated localized error message.
   */
  public static @NonNull String get(String key) {
    return get(key, emptyArgs);
  }

  public static @NonNull String get(@NonNull String key, @Nullable Object @NonNull[] args) {
    final String message = MESSAGES.getString(key);
    return MessageFormat.format(message, args);
  }
}
