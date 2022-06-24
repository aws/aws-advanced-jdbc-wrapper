/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package software.aws.rds.jdbc.proxydriver.hostlistprovider;

// A marker interface for providers that fetch node lists, and it never changes since after.
// An example of such provider is a provider that use connection string as a source.
public interface StaticHostListProvider { }
