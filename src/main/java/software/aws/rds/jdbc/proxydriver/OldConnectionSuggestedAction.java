/*
 *
 *  * AWS JDBC Proxy Driver
 *  * Copyright Amazon.com Inc. or affiliates.
 *  * See the LICENSE file in the project root for more information.
 *
 *
 */

package software.aws.rds.jdbc.proxydriver;

public enum OldConnectionSuggestedAction {
  NO_OPINION, // no strong suggestion about connection object
  DISPOSE, // suggestion to dispose a connection object; it may be overridden by "PRESERVE"
  PRESERVE // overrides any other opinions; the strongest suggestion
}
