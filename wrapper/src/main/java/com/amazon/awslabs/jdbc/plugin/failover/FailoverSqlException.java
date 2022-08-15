package com.amazon.awslabs.jdbc.plugin.failover;

import java.sql.SQLException;

public class FailoverSqlException extends SQLException {
  protected String exceptionMessage;

  public FailoverSqlException() {
    super();
  }

  public FailoverSqlException(String reason, String SQLState, int vendorCode) {
    super(reason, SQLState, vendorCode);
  }

  public FailoverSqlException(String reason, String SQLState) {
    super(reason, SQLState);
  }

  public FailoverSqlException(String reason) {
    super(reason);
  }

  public FailoverSqlException(Throwable cause) {
    super(cause);
  }

  public FailoverSqlException(String reason, Throwable cause) {
    super(reason, cause);
  }

  public FailoverSqlException(String reason, String sqlState, Throwable cause) {
    super(reason, sqlState, cause);
  }

  public FailoverSqlException(String reason, String sqlState, int vendorCode, Throwable cause) {
    super(reason, sqlState, vendorCode, cause);
  }

}
