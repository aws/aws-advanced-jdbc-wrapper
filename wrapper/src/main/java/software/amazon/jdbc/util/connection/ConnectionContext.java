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
import software.amazon.jdbc.util.PropertyUtils;

public class ConnectionContext {
  protected final static ConnectionUrlParser connectionUrlParser = new ConnectionUrlParser();
  protected final String url;
  protected final String protocol;
  protected final TargetDriverDialect driverDialect;
  protected final Properties props;
  protected Dialect dbDialect;

  public ConnectionContext(String url, TargetDriverDialect driverDialect, Properties props) {
    this.url = url;
    this.protocol = connectionUrlParser.getProtocol(url);
    this.driverDialect = driverDialect;
    this.props = props;
  }

  public String getUrl() {
    return url;
  }

  public String getProtocol() {
    return protocol;
  }

  public TargetDriverDialect getDriverDialect() {
    return driverDialect;
  }

  public Properties getProps() {
    return PropertyUtils.copyProperties(props);
  }

  public Dialect getDbDialect() {
    return dbDialect;
  }

  public void setDbDialect(Dialect dbDialect) {
    this.dbDialect = dbDialect;
  }
}
