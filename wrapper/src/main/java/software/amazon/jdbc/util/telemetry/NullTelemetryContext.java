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

package software.amazon.jdbc.util.telemetry;

public class NullTelemetryContext implements TelemetryContext {

  public NullTelemetryContext(String name) {
  }

  @Override
  public void setSuccess(boolean success) {
  }

  @Override
  public void setAttribute(String key, String value) {
  }

  @Override
  public void setException(Exception exception) {
  }

  @Override
  public String getName() {
    return null;
  }

  @Override
  public void closeContext() {
  }

}
