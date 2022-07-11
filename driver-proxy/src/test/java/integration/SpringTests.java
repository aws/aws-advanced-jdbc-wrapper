/*
*    Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
* 
*    Licensed under the Apache License, Version 2.0 (the "License").
*    You may not use this file except in compliance with the License.
*    You may obtain a copy of the License at
* 
*    http://www.apache.org/licenses/LICENSE-2.0
* 
*    Unless required by applicable law or agreed to in writing, software
*    distributed under the License is distributed on an "AS IS" BASIS,
*    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*    See the License for the specific language governing permissions and
*    limitations under the License.
*/

package integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import integration.util.TestSettings;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;
import javax.sql.DataSource;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

@Disabled
public class SpringTests {

  @Test
  public void testOpenConnection() throws SQLException, ClassNotFoundException {

    // Make sure that MySql driver class is loaded and registered at DriverManager
    Class.forName("com.mysql.cj.jdbc.Driver");

    if (!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
      software.aws.rds.jdbc.proxydriver.Driver.register();
    }

    JdbcTemplate jdbcTemplate = new JdbcTemplate(mysqlDataSource());

    int rnd = new Random().nextInt(100);
    int result = jdbcTemplate.queryForObject("SELECT " + rnd, Integer.class);
    assertEquals(rnd, result);
  }

  private DataSource mysqlDataSource() {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("software.aws.rds.jdbc.proxydriver.Driver");
    dataSource.setUrl(
        "aws-proxy-jdbc:mysql://"
            + TestSettings.mysqlServerName
            + "/"
            + TestSettings.mysqlDatabase);
    dataSource.setUsername(TestSettings.mysqlUser);
    dataSource.setPassword(TestSettings.mysqlPassword);

    Properties props = new Properties();
    props.setProperty("proxyDriverLoggerLevel", "ALL");
    dataSource.setConnectionProperties(props);

    return dataSource;
  }
}
