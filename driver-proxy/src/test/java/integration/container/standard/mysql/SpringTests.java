/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.container.standard.mysql;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Properties;
import java.util.Random;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

public class SpringTests extends StandardMysqlBaseTest {

  @Test
  public void testOpenConnection() {

    JdbcTemplate jdbcTemplate = new JdbcTemplate(mysqlDataSource());

    int rnd = new Random().nextInt(100);
    int result = jdbcTemplate.queryForObject("SELECT " + rnd, Integer.class);
    assertEquals(rnd, result);
  }

  private DataSource mysqlDataSource() {
    DriverManagerDataSource dataSource = new DriverManagerDataSource();
    dataSource.setDriverClassName("software.aws.rds.jdbc.proxydriver.Driver");
    dataSource.setUrl(getUrl());
    dataSource.setUsername(STANDARD_MYSQL_USERNAME);
    dataSource.setPassword(STANDARD_MYSQL_PASSWORD);

    Properties props = new Properties();
    props.setProperty("proxyDriverLoggerLevel", "ALL");
    dataSource.setConnectionProperties(props);

    return dataSource;
  }
}
