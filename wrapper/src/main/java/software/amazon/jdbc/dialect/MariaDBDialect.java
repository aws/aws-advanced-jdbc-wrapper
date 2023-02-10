package software.amazon.jdbc.dialect;

import software.amazon.jdbc.exceptions.ExceptionHandler;
import software.amazon.jdbc.exceptions.MariaDBExceptionHandler;
import software.amazon.jdbc.exceptions.MySQLExceptionHandler;

public class MariaDBDialect extends MySQLDialect implements DatabaseDialect {
  static final ExceptionHandler exceptionHandler = new MariaDBExceptionHandler();
  private static final String URL_SCHEME = "jdbc:mariadb://";

  @Override
  public ExceptionHandler getExceptionHandler() {
    return exceptionHandler;
  }
  public String getURLScheme() {
    return URL_SCHEME;
  }

}
