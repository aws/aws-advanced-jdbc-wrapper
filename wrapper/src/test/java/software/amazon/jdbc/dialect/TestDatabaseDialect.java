package software.amazon.jdbc.dialect;

public class TestDatabaseDialect extends DefaultDatabaseDialect implements DatabaseDialect {

  private final String urlScheme;
  public TestDatabaseDialect(String urlScheme){
    this.urlScheme = urlScheme;
  }
  @Override
  public String getURLScheme() {
    return urlScheme;
  }
}
