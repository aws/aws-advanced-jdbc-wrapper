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

package software.amazon.jdbc.util.connection;

import java.util.Properties;
import software.amazon.jdbc.dialect.Dialect;
import software.amazon.jdbc.targetdriverdialect.TargetDriverDialect;
import software.amazon.jdbc.util.ConnectionUrlParser;

public class ConnectionContext {
  protected static final ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();
  protected final String initialConnectionString;
  protected final String protocol;
  protected final TargetDriverDialect driverDialect;
  protected final Properties props;
  protected Dialect dbDialect;

  public ConnectionContext(String initialConnectionString, TargetDriverDialect driverDialect, Properties props) {
    this(initialConnectionString, connectionUrlParser.getProtocol(initialConnectionString), driverDialect, props);
  }

  public ConnectionContext(
      String initialConnectionString, String protocol, TargetDriverDialect driverDialect, Properties props) {
    this.initialConnectionString = initialConnectionString;
    this.protocol = protocol;
    this.driverDialect = driverDialect;
    this.props = props;
  }

  public String getInitialConnectionString() {
    return this.initialConnectionString;
  }

  public String getProtocol() {
    return this.protocol;
  }

  public TargetDriverDialect getDriverDialect() {
    return this.driverDialect;
  }

  public Properties getProps() {
    return this.props;
  }

  public Dialect getDbDialect() {
    return this.dbDialect;
  }

  public void setDbDialect(Dialect dbDialect) {
    this.dbDialect = dbDialect;
  }
}
