package integration.container.aurora.mysql;

import com.zaxxer.hikari.SQLExceptionOverride;

import java.sql.SQLException;

public class HikariCPSQLException implements SQLExceptionOverride {
  public Override adjudicate(final SQLException sqlException) {
    String sqlState = sqlException.getSQLState();
    if (sqlState.equalsIgnoreCase("08S02") ||
        sqlState.equalsIgnoreCase("08007")) {
      return Override.DO_NOT_EVICT;
    } else {
      return Override.CONTINUE_EVICT;
    }
  }
}

