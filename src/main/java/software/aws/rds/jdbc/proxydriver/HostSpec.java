/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class HostSpec {

    public static final int NO_PORT = 0;

    protected final String host;
    protected final int port;
    protected HostAvailability availability;
    protected HostRole role;
    protected Set<String> aliases = new HashSet<>();

    public HostSpec(String host) {
        this.host = host;
        this.port = NO_PORT;
        this.availability = HostAvailability.AVAILABLE;
        this.role = HostRole.WRITER;
    }

    public HostSpec(String host, int port) {
        this.host = host;
        this.port = port;
        this.availability = HostAvailability.AVAILABLE;
        this.role = HostRole.WRITER;
    }

    public HostSpec(String host, int port, HostRole role) {
        this.host = host;
        this.port = port;
        this.availability = HostAvailability.AVAILABLE;
        this.role = role;
    }

    public HostSpec(String host, int port, HostRole role, HostAvailability availability) {
        this.host = host;
        this.port = port;
        this.availability = availability;
        this.role = role;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public boolean isPortSpecified() { return port != NO_PORT; }

    public HostRole getRole() { return this.role; }

    public HostAvailability getAvailability() { return this.availability; }

    public Set<String> getAliases() { return Collections.unmodifiableSet(this.aliases); }

    public void addAlias(String alias) {
        this.aliases.add(alias);
    }

    public void removeAlias(String alias) {
        this.aliases.remove(alias);
    }

    public String getUrl() {
        return isPortSpecified() ? host + ":" + port : host;
    }

    public String toString() {
        return String.format("HostSpec[host=%s, port=%d]", this.host, this.port);
    }

}
