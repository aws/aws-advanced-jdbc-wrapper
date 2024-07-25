package software.amazon;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class EndpointPluginExample {
  private static final String USER = "user";
  private static final String PASSWORD = "password";

  public static void main(String[] args) throws SQLException, InterruptedException {
    AwsWrapperDataSource ds = new AwsWrapperDataSource();

    ds.setJdbcProtocol("jdbc:postgresql:");

    // Specify the driver-specific data source:
    ds.setTargetDataSourceClassName("org.postgresql.ds.PGSimpleDataSource");

    // Configure basic data source information:
    ds.setServerName("db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com");
    ds.setDatabase("test");
    ds.setServerPort("5432");

    // Configure the driver-specific and AWS JDBC Driver properties (optional):
    Properties targetDataSourceProps = new Properties();

    // Alternatively, instead of using the methods above to configure the basic data source information,
    // those properties can be set using the target data source properties:
    // targetDataSourceProps.setProperty("serverName", "db-identifier.cluster-XYZ.us-east-2.rds.amazonaws.com");
    // targetDataSourceProps.setProperty("database", "employees");
    // targetDataSourceProps.setProperty("serverPort", "5432");
    targetDataSourceProps.setProperty("wrapperPlugins", "limitless");

    // Configure any driver-specific properties:
//     targetDataSourceProps.setProperty("ssl", "true");

    // Configure any AWS JDBC Driver properties:
    targetDataSourceProps.setProperty("wrapperLoggerLevel", "ALL");

    ds.setTargetDataSourceProperties(targetDataSourceProps);

    // Try and make a connection:
    while(true) {
      try (final Connection conn = ds.getConnection(USER, PASSWORD);
           final Statement statement = conn.createStatement();
      ) {
        final ResultSet rs = statement.executeQuery("SELECT * from aurora_db_instance_identifier()");
        System.out.println(Util.getResult(rs));
      } catch (Exception e) {
        System.out.println(e);
      }
      Thread.sleep(5000);
    }

  }
}
