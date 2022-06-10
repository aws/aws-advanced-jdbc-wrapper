/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

public class HostSpec {

    public static final int NO_PORT = 0;

    protected final String host;
    protected final int port;

    public HostSpec(String host) {
        this.host = host;
        this.port = NO_PORT;
    }

    public HostSpec(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isPortSpecified() { return port != NO_PORT; }

    public String getUrl() {
        return isPortSpecified() ? host + ":" + port : host;
    }

    public String toString() {
        return String.format("HostSpec[host=%s, port=%d]", this.host, this.port);
    }

}
