/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver;

public interface HostListProviderService {
    boolean isDefaultHostListProvider();
    void setHostListProvider(HostListProvider hostListProvider);
}
