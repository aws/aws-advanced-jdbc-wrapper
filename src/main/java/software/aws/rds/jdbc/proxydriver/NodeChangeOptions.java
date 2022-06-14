/*
 *
 *  * AWS JDBC Proxy Driver
 *  * Copyright Amazon.com Inc. or affiliates.
 *  * See the LICENSE file in the project root for more information.
 *
 */

package software.aws.rds.jdbc.proxydriver;

public enum NodeChangeOptions {
  HOSTNAME,
  PROMOTED_TO_WRITER,
  PROMOTED_TO_READER,
  WENT_UP,
  WENT_DOWN,
  CONNECTION_OBJECT_CHANGED,
  NODE_ADDED,
  NODE_CHANGED,
  NODE_DELETED
}
