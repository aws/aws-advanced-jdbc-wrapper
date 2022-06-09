package software.aws.rds.jdbc.proxydriver;

import java.util.concurrent.Callable;

public interface JdbcCallable <T, E extends Exception> {
    T call() throws E;
}
