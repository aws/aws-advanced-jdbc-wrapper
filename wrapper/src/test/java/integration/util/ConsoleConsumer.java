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

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;
import software.amazon.jdbc.util.Messages;

public class ConsoleConsumer
    extends BaseConsumer<org.testcontainers.containers.output.Slf4jLogConsumer> {

  private boolean separateOutputStreams;

  public ConsoleConsumer() {
    this(false);
  }

  public ConsoleConsumer(boolean separateOutputStreams) {
    this.separateOutputStreams = separateOutputStreams;
  }

  @Override
  public void accept(OutputFrame outputFrame) {
    final OutputFrame.OutputType outputType = outputFrame.getType();

    final String utf8String = outputFrame.getUtf8String();

    switch (outputType) {
      case END:
        break;
      case STDOUT:
        System.out.print(utf8String);
        break;
      case STDERR:
        if (separateOutputStreams) {
          System.err.print(utf8String);
        } else {
          System.out.print(utf8String);
        }
        break;
      default:
        throw new IllegalArgumentException(
            Messages.get(
                "ConsoleConsumer.unexpectedOutputType",
                new Object[] {outputType}));
    }
  }
}
