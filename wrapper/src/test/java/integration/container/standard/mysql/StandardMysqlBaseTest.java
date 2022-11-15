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

package integration.container.standard.mysql;

import com.mysql.cj.conf.PropertyKey;
import integration.container.standard.StandardBaseTest;
import java.util.Properties;
import org.junit.jupiter.api.BeforeAll;

public class StandardMysqlBaseTest extends StandardBaseTest {

  @BeforeAll
  public static void setupMysqlDatabaseTests() {
    QUERY_FOR_HOSTNAME = "SELECT @@hostname";
  }

  @Override
  protected Properties initDefaultProps() {
    final Properties props = initDefaultPropsNoTimeouts();
    props.setProperty(PropertyKey.connectTimeout.getKeyName(), "3000");
    props.setProperty(PropertyKey.socketTimeout.getKeyName(), "3000");

    return props;
  }

  @Override
  protected Properties initDefaultPropsNoTimeouts() {
    final Properties props = new Properties();
    props.setProperty(PropertyKey.USER.getKeyName(), STANDARD_USERNAME);
    props.setProperty(PropertyKey.PASSWORD.getKeyName(), STANDARD_PASSWORD);
    props.setProperty(PropertyKey.tcpKeepAlive.getKeyName(), Boolean.FALSE.toString());

    return props;
  }
}
