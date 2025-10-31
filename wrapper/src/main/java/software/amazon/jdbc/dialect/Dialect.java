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

package software.amazon.jdbc.dialect;

import java.sql.Connection;
import java.util.EnumSet;
import java.util.List;
import java.util.Properties;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.HostSpec;
import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.plugin.failover.FailoverRestriction;

public interface Dialect {

  boolean isDialect(Connection connection);

  int getDefaultPort();

  List</* dialect code */ String> getDialectUpdateCandidates();

  ExceptionHandler getExceptionHandler();

  HostListProviderSupplier getHostListProvider();

  String getHostAliasQuery();

  void prepareConnectProperties(
      final @NonNull Properties connectProperties, final @NonNull String protocol, final @NonNull HostSpec hostSpec);

  EnumSet<FailoverRestriction> getFailoverRestrictions();

  String getServerVersionQuery();
}
