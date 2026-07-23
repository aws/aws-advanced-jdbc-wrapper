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

package software.amazon.jdbc.plugin.readwritesplitting.signal;

import org.checkerframework.checker.nullness.qual.Nullable;
import software.amazon.jdbc.JdbcMethod;
import software.amazon.jdbc.plugin.readwritesplitting.RwSplitContext;

/**
 * Classic {@link RoutingSignal}: routes purely on {@code Connection.setReadOnly(boolean)}.
 * {@code setReadOnly(true)} resolves to {@link TargetRole#READER}, {@code setReadOnly(false)} to
 * {@link TargetRole#WRITER}; every other method resolves to {@link TargetRole#NO_DECISION}.
 */
public class ReadOnlyFlagSignal implements RoutingSignal {

  @Override
  public TargetRole resolve(
      final RwSplitContext ctx, final String methodName, final @Nullable Object[] args) {
    if (JdbcMethod.CONNECTION_SETREADONLY.methodName.equals(methodName)
        && args != null
        && args.length > 0
        && args[0] instanceof Boolean) {
      return ((Boolean) args[0]) ? TargetRole.READER : TargetRole.WRITER;
    }
    return TargetRole.NO_DECISION;
  }
}
