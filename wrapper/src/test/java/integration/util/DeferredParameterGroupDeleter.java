/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package integration.util;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.logging.Logger;
import software.amazon.awssdk.services.rds.model.DbParameterGroupNotFoundException;
import software.amazon.awssdk.services.rds.model.InvalidDbParameterGroupStateException;
import software.amazon.jdbc.util.StringUtils;

/**
 * Deletes custom test parameter groups in the background, retrying for as long as RDS still reports
 * them as in use.
 *
 * <p>A parameter group cannot be deleted while a DB cluster or a DB instance still references it, and
 * {@code DeleteDBCluster} / {@code DeleteDBInstance} only <em>start</em> the deletion: the database
 * keeps referencing its parameter group for several more minutes. Test teardown deliberately does not
 * wait for the database to disappear, because waiting would add those minutes to the teardown of every
 * environment in a run, so the parameter group deletion has to outlive the teardown that requests it.
 * Deleting the group inline instead fails with {@link InvalidDbParameterGroupStateException} and leaks
 * the group, which eventually exhausts the region's parameter group quota.
 *
 * <p>Each request is therefore queued here and retried on a daemon thread until RDS accepts it.
 * Whatever is still pending when the JVM exits is awaited for a bounded time and then reported at
 * WARNING. {@link AuroraTestUtility#testClusterParameterGroupsCleanUp()} and
 * {@link AuroraTestUtility#testDbParameterGroupsCleanUp()} remain the backstop for groups that outlive
 * the run entirely, for instance when the run is killed.
 */
public class DeferredParameterGroupDeleter {

  private static final Logger LOGGER = Logger.getLogger(DeferredParameterGroupDeleter.class.getName());

  // How long to keep retrying a single group. The clock starts when the deletion is queued, which is
  // before the database deletion has even been requested, so the budget has to cover the whole teardown.
  // A blue/green teardown is the worst case: it removes a deployment and two clusters, and each
  // "cluster deleted" waiter alone allows 60 minutes. Three hours covers that with room to spare and
  // still bounds a database that never goes away. An attempt is a single API call, so the budget is cheap.
  private static final long RETRY_TIMEOUT_MS = TimeUnit.HOURS.toMillis(3);
  private static final long RETRY_DELAY_MS = TimeUnit.SECONDS.toMillis(30);

  // How long the JVM is held at exit waiting for outstanding deletions. This is a short courtesy for
  // deletions that are nearly done, not a guarantee: deleting a database takes longer than this, so the
  // last environment of a run will often still have its group pending. Waiting it out would put the
  // minutes this class exists to avoid straight back into the run, and the periodic sweep in
  // AuroraTestUtility collects whatever is left.
  private static final long SHUTDOWN_TIMEOUT_MS = TimeUnit.MINUTES.toMillis(2);
  private static final long SHUTDOWN_POLL_MS = TimeUnit.SECONDS.toMillis(5);

  private static final ExecutorService executor = Executors.newCachedThreadPool(runnable -> {
    final Thread thread = new Thread(runnable, "deferred-parameter-group-deleter");
    // Daemon threads never hold the JVM open by themselves; the shutdown hook below is what gives
    // outstanding deletions a bounded chance to finish.
    thread.setDaemon(true);
    return thread;
  });

  private static final Set<String> pending = ConcurrentHashMap.newKeySet();
  private static final AtomicBoolean shutdownHookRegistered = new AtomicBoolean(false);

  private DeferredParameterGroupDeleter() {
  }

  /**
   * Queues a DB cluster parameter group for deletion, retrying in the background until the cluster that
   * references it has finished being deleted. Does nothing when there is no group to delete.
   *
   * @param auroraUtil the utility whose RDS client performs the deletion
   * @param groupName  the name of the DB cluster parameter group to delete
   */
  public static void deleteClusterParameterGroupLater(
      final AuroraTestUtility auroraUtil, final String groupName) {
    if (auroraUtil == null || StringUtils.isNullOrEmpty(groupName)) {
      return;
    }
    submit("cluster parameter group " + groupName, groupName,
        auroraUtil::deleteCustomClusterParameterGroup);
  }

  /**
   * Queues a DB parameter group for deletion, retrying in the background until the instance that
   * references it has finished being deleted. Does nothing when there is no group to delete.
   *
   * @param auroraUtil the utility whose RDS client performs the deletion
   * @param groupName  the name of the DB parameter group to delete
   */
  public static void deleteDbParameterGroupLater(
      final AuroraTestUtility auroraUtil, final String groupName) {
    if (auroraUtil == null || StringUtils.isNullOrEmpty(groupName)) {
      return;
    }
    submit("DB parameter group " + groupName, groupName, auroraUtil::deleteCustomDbParameterGroup);
  }

  private static void submit(
      final String description, final String groupName, final Consumer<String> deleteAction) {

    registerShutdownHook();

    if (!pending.add(description)) {
      // Already queued by an earlier teardown; the running task covers this group.
      return;
    }

    try {
      executor.submit(() -> {
        try {
          final boolean deleted = RetryHelper.retryUntil(
              RETRY_TIMEOUT_MS, RETRY_DELAY_MS, () -> tryDelete(description, groupName, deleteAction));
          if (!deleted) {
            LOGGER.warning(String.format(
                "%s was still in use after %d minutes, so it could not be deleted. It is left for the "
                    + "periodic test resource cleanup to remove.",
                description, TimeUnit.MILLISECONDS.toMinutes(RETRY_TIMEOUT_MS)));
          }
        } finally {
          pending.remove(description);
        }
      });
    } catch (Exception ex) {
      // Nothing is going to run the task, so do not leave an entry behind for the shutdown hook to wait on.
      pending.remove(description);
      LOGGER.warning(String.format("Could not queue the deletion of %s. %s", description, ex));
    }
  }

  /**
   * Returns true when there is nothing left to do, either because the group is gone or because retrying
   * cannot help, and false when the group is still attached and the attempt is worth repeating.
   */
  private static boolean tryDelete(
      final String description, final String groupName, final Consumer<String> deleteAction) {

    try {
      deleteAction.accept(groupName);
      LOGGER.finest(() -> "Deleted " + description + ".");
      return true;

    } catch (DbParameterGroupNotFoundException ex) {
      // Already gone: deleted by the periodic cleanup, or never actually created. Both
      // DeleteDBClusterParameterGroup and DeleteDBParameterGroup report this as DBParameterGroupNotFound.
      LOGGER.finest(() -> description + " does not exist, nothing to delete.");
      return true;

    } catch (InvalidDbParameterGroupStateException ex) {
      // The database that references the group is still being deleted. This is the expected outcome of the
      // first attempts, so it is the one case worth retrying and is not worth a warning on its own.
      LOGGER.finest(() -> "Cannot delete " + description + " yet, it is still in use. Retrying.");
      return false;

    } catch (Exception ex) {
      // The RDS client already retries throttling and transient API failures internally (see
      // AuroraTestUtility's adaptive retry strategy), so anything surfacing here is unlikely to fix itself
      // -- a missing permission or expired credentials, say. Report it once instead of spending the whole
      // budget re-issuing a call that keeps failing.
      LOGGER.warning(String.format("Could not delete %s. %s", description, ex));
      return true;
    }
  }

  private static void registerShutdownHook() {
    if (!shutdownHookRegistered.compareAndSet(false, true)) {
      return;
    }
    Runtime.getRuntime().addShutdownHook(
        new Thread(DeferredParameterGroupDeleter::awaitPendingDeletions,
            "deferred-parameter-group-deleter-shutdown"));
  }

  private static void awaitPendingDeletions() {
    if (pending.isEmpty()) {
      return;
    }

    LOGGER.info(String.format(
        "Waiting up to %d minutes for %d parameter group deletion(s) to complete: %s",
        TimeUnit.MILLISECONDS.toMinutes(SHUTDOWN_TIMEOUT_MS), pending.size(), pending));

    if (!RetryHelper.retryUntil(SHUTDOWN_TIMEOUT_MS, SHUTDOWN_POLL_MS, pending::isEmpty)) {
      LOGGER.warning(String.format(
          "Exiting with %d parameter group(s) not deleted: %s. They are left for the periodic test "
              + "resource cleanup to remove.",
          pending.size(), pending));
    }
  }
}
