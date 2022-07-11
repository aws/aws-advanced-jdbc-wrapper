/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

package integration.util;

import org.testcontainers.containers.output.BaseConsumer;
import org.testcontainers.containers.output.OutputFrame;

public class ConsoleConsumer
    extends BaseConsumer<org.testcontainers.containers.output.Slf4jLogConsumer> {

  private boolean separateOutputStreams;

  public ConsoleConsumer() {
    this(false);
  }

  public ConsoleConsumer(boolean separateOutputStreams) {
    this.separateOutputStreams = separateOutputStreams;
  }

  public ConsoleConsumer withSeparateOutputStreams() {
    this.separateOutputStreams = true;
    return this;
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
        throw new IllegalArgumentException("Unexpected outputType " + outputType);
    }
  }
}
