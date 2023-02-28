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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;
import org.checkerframework.checker.nullness.qual.NonNull;
import software.amazon.jdbc.util.RdsUrlType;
import software.amazon.jdbc.util.RdsUtils;

public class HikariPooledConnectionProvider implements PooledConnectionProvider {

  private final RdsUtils rdsUtils = new RdsUtils();
  private final Map<String, HikariDataSource> databasePools = new ConcurrentHashMap<>();

  public HikariPooledConnectionProvider(
      BiFunction<HostSpec, Properties, HikariConfig> poolConfigurator) {
    this.poolConfigurator = poolConfigurator;
  }
  private final BiFunction<HostSpec, Properties, HikariConfig> poolConfigurator;

  @Override
  public boolean acceptsUrl(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props) {
    final RdsUrlType urlType = rdsUtils.identifyRdsType(hostSpec.getHost());
    return RdsUrlType.RDS_INSTANCE.equals(urlType);
  }

  @Override
  public Connection connect(
      @NonNull String protocol, @NonNull HostSpec hostSpec, @NonNull Properties props)
      throws SQLException {
    HikariDataSource ds = databasePools.computeIfAbsent(
        hostSpec.getUrl(), url -> new HikariDataSource(poolConfigurator.apply(hostSpec,  props)));
    return ds.getConnection();
  }

  @Override
  public Connection connect(
      @NonNull String url, @NonNull Properties props) throws SQLException {
    return null;
  }

  @Override
  public void releaseResources() {
    databasePools.forEach((String url, HikariDataSource ds) -> ds.close());
    databasePools.clear();
  }
}
