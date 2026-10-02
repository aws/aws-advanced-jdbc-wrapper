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

package integration.orchestra;

import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Logger;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.CompositionAction;
import software.amazon.orchestra.EnvConfiguration;
import software.amazon.orchestra.OrchestraException;
import software.amazon.orchestra.contract.Database;
import software.amazon.orchestra.instruments.DatabaseState;
import software.amazon.orchestra.instruments.docker.JavaTestContainerConfiguration;
import software.amazon.orchestra.instruments.docker.JavaTestContainerInstrumentDefinition;
import software.amazon.orchestra.instruments.docker.JavaTestContainerState;

/**
 * Runs Hibernate ORM's own test suite in the test container, then collects what it produced.
 *
 * <p>Written here rather than using Orchestra's {@code GradleTestContainerRun} for one reason that decides it:
 * the command has to name the database, and the database's hostname is only known once the composition is
 * provisioned. {@code GradleTestContainerRun} builds its command from configuration, and a configuration
 * object cannot see the composition - so the alias would have to be assumed rather than read, and the
 * instrument deliberately disambiguates aliases when a composition holds more than one database.
 *
 * <p>Two steps rather than one, because Hibernate writes its reports inside its own module tree - a
 * {@code target/reports/tests} directory under each of a dozen modules - and nothing on the host can reach
 * them there. The script gathers them into two archives under the one bound directory. It is run as a second
 * step rather than an
 * {@code afterUse} action so that it only runs when the suite has finished, whatever the outcome, in the same
 * action that knows where the suite ran.
 *
 * <p>Output is streamed rather than collected. Testcontainers' {@code execInContainer} returns the whole
 * output as a string once the command finishes, which for a suite of this size means both holding all of it in
 * memory and seeing none of it until the end - and this run is measured in hours, so silence for its duration
 * is indistinguishable from a hang. The harness streams for the same reason.
 */
public class HibernateSuiteRun implements CompositionAction {

  private static final Logger LOGGER = Logger.getLogger(HibernateSuiteRun.class.getName());

  /** Where the results script was copied, outside the checkout because it collects from all of it. */
  private static final String COLLECT_SCRIPT = "/app/collect_test_results.sh";

  private final String gradleTask;

  /**
   * Creates the action.
   *
   * @param gradleTask the task to run in Hibernate's build, normally {@code test}
   */
  public HibernateSuiteRun(final String gradleTask) {
    this.gradleTask = gradleTask;
  }

  @Override
  public void execute(final Composition composition, final EnvConfiguration configuration) throws Exception {
    final JavaTestContainerConfiguration config = (JavaTestContainerConfiguration) configuration;

    final JavaTestContainerState testContainer = composition.getInstrumentState(
        JavaTestContainerInstrumentDefinition.class, JavaTestContainerState.class);
    final DatabaseState database =
        composition.getInstrumentState(Database.class, DatabaseState.class);

    final List<String> command = suiteCommand(config, database);
    LOGGER.info(() -> "Running Hibernate's suite in " + testContainer.workingDirectory() + ": "
        + String.join(" ", command));

    final long exitCode = stream(testContainer.container(), command);

    // Before the exit code is checked, because a failing suite is exactly when its reports are wanted. The
    // archives are also how a failure is diagnosed at all: the console output of a suite this size is
    // unreadable, and the per-test XML is what says which of Hibernate's tests failed.
    collectResults(testContainer.container());

    if (exitCode != 0) {
      throw new OrchestraException(
          "Hibernate's test suite failed in the test container with exit code " + exitCode
              + ". Its reports were collected into build/test-results; the console output above is the "
              + "Gradle run itself.");
    }
  }

  /**
   * Builds the command that runs Hibernate's suite against this composition's database.
   *
   * <p>The four database properties are what Hibernate's {@code pg_amazon_ci} profile reads, and the host is
   * the container-internal one: the suite runs inside the container and reaches the database across the Docker
   * network, where a published port would resolve to the test container itself.
   *
   * <p>The locale, timezone and encoding properties are the harness's, and they are not incidental. Hibernate
   * has tests that assert on formatted dates and on message text, so a run inherits failures from the host's
   * locale unless it is pinned - which is the kind of failure that reproduces on one machine and not another.
   *
   * <p>{@code --no-parallel} and {@code --no-daemon} come from the configuration's Gradle arguments, and
   * {@code --no-build-cache} with them: this build runs once in a container that is then destroyed, so a cache
   * has nothing to serve and a daemon nothing to keep warm.
   */
  private List<String> suiteCommand(
      final JavaTestContainerConfiguration config, final DatabaseState database) {

    final List<String> command = new ArrayList<>();
    command.add(config.getGradleCommand());
    command.add(this.gradleTask);

    command.add("-DdbHost=" + database.internalHostname());
    command.add("-DdbUser=" + database.username());
    command.add("-DdbPass=" + database.password());
    command.add("-DdbName=" + database.databaseName());

    command.addAll(config.getGradleArguments());
    command.add("--no-build-cache");

    // Plain console, because the log is read rather than watched. Gradle's rich console redraws a progress
    // area using carriage returns instead of newlines, and a reader assembling lines therefore sees nothing
    // for as long as a task runs: the first run of this action went eight minutes without a line while the
    // container sat at 240% CPU, which is indistinguishable from a hang.
    command.add("--console=plain");

    command.add("-Duser.language=en");
    command.add("-Duser.country=US");
    command.add("-Duser.timezone=UTC");
    command.add("-Dfile.encoding=UTF-8");

    // Which of Hibernate's database profiles to use, from the local.databases.gradle copied into the clone.
    // This is what puts the wrapper on the connection path rather than the plain PostgreSQL driver.
    command.add("-Pdb=pg_amazon_ci");

    return command;
  }

  /**
   * Gathers Hibernate's reports into the bound output directory.
   *
   * <p>Failures are logged rather than thrown. The archives are diagnostic output, so a suite that ran and a
   * script that could not tar its reports is a worse thing to report as "the composition failed" than as a
   * warning next to the suite's own result.
   *
   * <p>Invoked through {@code bash} by absolute path, not as {@code ./collect_test_results.sh}: the working
   * directory is Hibernate's checkout while the script sits above it, and the image is Alpine, whose default
   * shell is not bash.
   */
  private void collectResults(final GenericContainer<?> container) {
    try {
      // Piped through tr rather than run directly, because the script is copied from the developer's working
      // tree and a Windows checkout can hold it with CRLF endings. bash then reports
      // "collect_test_results.sh: line 2: $'\r': command not found" and the tar that follows receives no
      // input, so a run collects nothing - which is how this was found. The repository already asks git for
      // LF on shell scripts, but that only applies when the file is checked out, and a working tree that
      // predates the rule keeps its endings. Stripping them here makes the run independent of that.
      final long exitCode = stream(container,
          List.of("bash", "-c", "tr -d '\\r' < " + COLLECT_SCRIPT + " | bash -s"));
      if (exitCode != 0) {
        LOGGER.warning(() -> "Collecting Hibernate's test results exited with " + exitCode
            + ", so build/test-results may hold no archives for this run.");
      }
    } catch (final Exception ex) {
      LOGGER.warning(() -> "Could not collect Hibernate's test results: " + ex);
    }
  }

  /**
   * Runs a command in the container, logging its output as it arrives, and returns its exit code.
   *
   * <p>Through docker-java directly rather than {@code GenericContainer.execInContainer}, which buffers the
   * entire output and returns it at the end. For a run of this length that is the difference between a live
   * log and hours of silence.
   *
   * <p>No working directory is set, so the command inherits the container's own - Hibernate's checkout. That
   * is the same directory {@code GradleTestContainerAction} relies on, and keeping it implicit means the
   * configuration is the single place that decides where the suite runs.
   */
  private static long stream(final GenericContainer<?> container, final List<String> command)
      throws Exception {

    final ExecCreateCmdResponse created = container.getDockerClient()
        .execCreateCmd(container.getContainerId())
        .withAttachStdout(true)
        .withAttachStderr(true)
        .withCmd(command.toArray(new String[0]))
        .exec();

    // One assembler per stream rather than one shared, because the two are delivered independently and a
    // shared buffer would splice a half-written stdout line into a stderr one.
    final LineLogger out = new LineLogger();
    final LineLogger err = new LineLogger();

    try (FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
      callback.addConsumer(OutputFrame.OutputType.STDOUT, out);
      callback.addConsumer(OutputFrame.OutputType.STDERR, err);

      container.getDockerClient().execStartCmd(created.getId()).exec(callback).awaitCompletion();
    } finally {
      // Whatever the command wrote without a final newline, which includes the last line of a process that
      // was killed - the most interesting line there is.
      out.flush();
      err.flush();
    }

    final Long exitCode =
        container.getDockerClient().inspectExecCmd(created.getId()).exec().getExitCodeLong();
    return exitCode == null ? -1 : exitCode;
  }

  /**
   * Reassembles a container's output stream into lines before logging it.
   *
   * <p>Because a docker frame is a chunk of the stream rather than a line: nothing guarantees a frame ends at
   * a newline, so logging one frame per entry can split a line across entries or join two. Testcontainers'
   * own output consumers assemble lines for the same reason. A log of a suite this long is only useful if its
   * lines are intact, since it is read by searching it.
   *
   * <p>Not shared between streams, and not thread-safe on purpose: one instance per output type, each written
   * by the callback thread delivering that type. A single synchronised buffer would serialise the two streams
   * and still interleave partial lines.
   */
  private static final class LineLogger implements Consumer<OutputFrame> {

    private final StringBuilder pending = new StringBuilder();

    @Override
    public void accept(final OutputFrame frame) {
      final String chunk = frame.getUtf8String();
      if (chunk == null || chunk.isEmpty()) {
        return;
      }

      this.pending.append(chunk);

      // Either terminator ends a line. A bare carriage return is how a console redraws in place rather than
      // advancing, so treating only newlines as boundaries holds all of that output back indefinitely - and
      // the command being run decides which it uses, not this class.
      int end = indexOfLineEnd(this.pending);
      while (end >= 0) {
        emit(this.pending.substring(0, end));
        this.pending.delete(0, end + 1);
        end = indexOfLineEnd(this.pending);
      }
    }

    /** Returns the position of the first line terminator, or -1. */
    private static int indexOfLineEnd(final CharSequence text) {
      for (int i = 0; i < text.length(); i++) {
        final char c = text.charAt(i);
        if (c == '\n' || c == '\r') {
          return i;
        }
      }
      return -1;
    }

    /** Logs anything left without a trailing newline. */
    void flush() {
      if (this.pending.length() > 0) {
        emit(this.pending.toString());
        this.pending.setLength(0);
      }
    }

    private static void emit(final String line) {
      final String trimmed = line.replaceAll("\\s+$", "");
      if (trimmed.isEmpty()) {
        return;
      }
      LOGGER.info(() -> "[hibernate] " + trimmed);
    }
  }
}
