package software.amazon.jdbc.benchmarks;

import software.amazon.jdbc.dialect.DefaultDatabaseDialect;

public class JMHDatabaseDialect extends DefaultDatabaseDialect {
  final String protocol;
  public JMHDatabaseDialect(String protocol) {
    this.protocol = protocol;
  }
  public String getURLScheme() {
    return protocol;
  }

}
