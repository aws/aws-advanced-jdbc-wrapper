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

package software.amazon.jdbc.exceptions;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;

public interface ExceptionHandler {

  /**
   * The method determines whether provided throwable is about any network issues.
   *
   * @deprecated Use similar method below that accepts throwable and target driver dialect.
   */
  @Deprecated()
  boolean isNetworkException(Throwable throwable);

  boolean isNetworkException(Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect);

  boolean isNetworkException(String sqlState);

  boolean isLoginException(String sqlState);

  /**
   * The method determines whether provided throwable is about any login or authentication issues.
   *
   * @deprecated Use similar method below that accepts throwable and target driver dialect.
   */
  @Deprecated()
  boolean isLoginException(Throwable throwable);

  boolean isLoginException(Throwable throwable, @Nullable TargetDriverDialect targetDriverDialect);
}
