/*
 *
 *  * AWS JDBC Proxy Driver
 *  * Copyright Amazon.com Inc. or affiliates.
 *  * See the LICENSE file in the project root for more information.
 *
 */

package software.aws.rds.jdbc.proxydriver.hostlistprovider;

// A marker interface for providers that can fetch a host list, and it changes depending on database status
// A good example of such provider would be DB cluster provider (Aurora DB clusters, patroni DB clusters, etc.)
// where cluster topology (nodes, their roles, their statuses) changes over time.
public interface DynamicHostListProvider { }
