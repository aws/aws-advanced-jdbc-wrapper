package software.aws.rds.jdbc.proxydriver;

public interface JdbcRunnable <E extends Exception> {
    void call() throws E;
}
