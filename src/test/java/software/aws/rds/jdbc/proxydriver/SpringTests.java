package software.aws.rds.jdbc.proxydriver;

import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import software.aws.rds.jdbc.proxydriver.util.TestSettings;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SpringTests {

    @Test
    public void testOpenConnection() throws SQLException, ClassNotFoundException {

        // Make sure that MySql driver class is loaded and registered at DriverManager
        Class.forName("com.mysql.cj.jdbc.Driver");

        if(!software.aws.rds.jdbc.proxydriver.Driver.isRegistered()) {
            software.aws.rds.jdbc.proxydriver.Driver.register();
        }

        JdbcTemplate jdbcTemplate = new JdbcTemplate(mysqlDataSource());

        int rnd = new Random().nextInt();
        int result = jdbcTemplate.queryForObject("SELECT " + rnd, Integer.class);
        assertEquals(rnd, result);
    }

    private DataSource mysqlDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("software.aws.rds.jdbc.proxydriver.Driver");
        dataSource.setUrl("aws-proxy-jdbc:mysql://" + TestSettings.mysqlServerName + "/" + TestSettings.mysqlDatabase);
        dataSource.setUsername(TestSettings.mysqlUser);
        dataSource.setPassword(TestSettings.mysqlPassword);

        Properties props = new Properties();
        //props.setProperty("proxyDriverLoggerLevel", "ALL");
        dataSource.setConnectionProperties(props);

        return dataSource;
    }
}
