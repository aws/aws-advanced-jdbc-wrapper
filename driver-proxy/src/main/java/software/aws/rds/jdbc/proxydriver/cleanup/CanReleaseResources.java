/*
 *
 *  * AWS JDBC Proxy Driver
 *  * Copyright Amazon.com Inc. or affiliates.
 *  * See the LICENSE file in the project root for more information.
 *
 */

package software.aws.rds.jdbc.proxydriver.cleanup;

public interface CanReleaseResources {

  /**
   * An object that implements this interface should release all acquired resources assuming it may
   * be disposed at any time. This method may be called more than once.
   *
   * <p>Calling this method does NOT mean that an object is disposing and there won't be any further
   * calls. An object should keep its functional state after calling this method.
   */
  void releaseResources();
}
