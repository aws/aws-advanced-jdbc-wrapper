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

import java.text.MessageFormat;
import java.util.MissingResourceException;
import java.util.ResourceBundle;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Retrieves test-harness messages without packaging them in the released driver jar. */
public final class TestMessages {

  private static final ResourceBundle TEST_MESSAGES =
      ResourceBundle.getBundle("aws_advanced_jdbc_wrapper_test_messages");
  private static final Object[] EMPTY_ARGS = {};

  private TestMessages() {
  }

  public static String get(final String key) {
    return get(key, EMPTY_ARGS);
  }

  public static String get(final String key, final @Nullable Object[] args) {
    try {
      return MessageFormat.format(TEST_MESSAGES.getString(key), args);
    } catch (final MissingResourceException e) {
      return Messages.get(key, args);
    }
  }
}
