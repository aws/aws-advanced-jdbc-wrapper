package integration.container.standard.mysql.mariadbdriver;

import org.junit.platform.suite.api.SelectClasses;
import org.junit.platform.suite.api.Suite;

// Tests will run in order of top to bottom.
// To add additional tests, append it inside SelectClasses, comma-separated
@Suite
@SelectClasses({
    StandardMysqlIntegrationTest.class,
    DataCachePluginTests.class,
    DataSourceTests.class,
    HikariTests.class,
    LogQueryPluginTests.class,
    SpringTests.class
})
public class MariadbStandardMysqlTestSuite {

}
