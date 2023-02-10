package software.amazon.jdbc.dialect;

import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.MySQLExceptionHandler;
import software.amazon.jdbc.exceptions.PgExceptionHandler;

public class PostgreSQLDialect implements DatabaseDialect {
  private static final ExceptionHandler exceptionHandler = new PgExceptionHandler();
  private static final String PG_RETRIEVE_TOPOLOGY_SQL =
      "SELECT SERVER_ID, SESSION_ID FROM aurora_replica_status() "
          // filter out nodes that haven't been updated in the last 5 minutes
          + "WHERE EXTRACT(EPOCH FROM(NOW() - LAST_UPDATE_TIMESTAMP)) <= 300 OR SESSION_ID = 'MASTER_SESSION_ID' "
          + "ORDER BY LAST_UPDATE_TIMESTAMP";
  private static final String PG_RETRIEVE_HOST_PORT_SQL =
      "SELECT CONCAT(inet_server_addr(), ':', inet_server_port())";

  private static final String POSTGRESQL_READONLY_QUERY = "SELECT pg_is_in_recovery() AS is_reader";
  private static final String READ_ONLY_COLUMN_NAME = "is_reader";
  private static final String PG_GET_INSTANCE_NAME_SQL = "SELECT aurora_db_instance_identifier()";
  private static final String PG_INSTANCE_NAME_COL = "aurora_db_instance_identifier";
  private static final int PG_PORT = 5432;
  private static final String URL_SCHEME="jdbc:postgresql://";
  @Override
  public String getInstanceNameQuery() {
    return PG_GET_INSTANCE_NAME_SQL;
  }
  public String getInstanceNameColumn() {
    return PG_INSTANCE_NAME_COL;
  }
  @Override
  public boolean isSupported() {
    return true;
  }
  public int getDefaultPort() {
    return PG_PORT;
  }

  @Override
  public ExceptionHandler getExceptionHandler() {
    return exceptionHandler;
  }
  public String getTopologyQuery() {
    return PG_RETRIEVE_TOPOLOGY_SQL;
  }

  @Override
  public String getReadOnlyQuery() {
    return POSTGRESQL_READONLY_QUERY;
  }

  @Override
  public String getReadOnlyColumnName() {
    return READ_ONLY_COLUMN_NAME;
  }

  @Override
  public String getHostPortQuery() {
    return PG_RETRIEVE_HOST_PORT_SQL;
  }

  @Override
  public String getURLScheme() {
    return URL_SCHEME;
  }

}
