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

import org.testcontainers.containers.Container;
import software.amazon.jdbc.util.Messages;
import software.amazon.orchestra.Composition;
import software.amazon.orchestra.CompositionAction;
import software.amazon.orchestra.EnvConfiguration;
import software.amazon.orchestra.OrchestraException;
import software.amazon.orchestra.instruments.docker.JavaTestContainerInstrumentDefinition;
import software.amazon.orchestra.instruments.docker.JavaTestContainerState;

/** Expands the host's resolved Gradle modules before an isolated test container runs its build. */
public class PrepareGradleRuntime implements CompositionAction {

  @Override
  public void execute(final Composition composition, final EnvConfiguration configuration)
      throws Exception {

    final JavaTestContainerState state = composition.getInstrumentState(
        JavaTestContainerInstrumentDefinition.class, JavaTestContainerState.class);

    final String command = extractionCommand();
    final Container.ExecResult result = state.container().execInContainer("sh", "-c", command);

    if (result.getExitCode() != 0) {
      throw new OrchestraException(Messages.get(
          "PrepareGradleRuntime.failed",
          new Object[] {composition.getDisplayName(), result.getExitCode(), result.getStderr()}));
    }
  }

  public static String extractionCommand() {
    return GradleRuntime.cacheExtractionCommand();
  }
}
