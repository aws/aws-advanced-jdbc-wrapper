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

public class TelemetryConst {
  public static final String TRACE_NAME_ANNOTATION = "traceName";
  public static final String SOURCE_TRACE_ANNOTATION = "sourceTraceId";
  public static final String PARENT_TRACE_ANNOTATION = "parentTraceId";
  public static final String EXCEPTION_TYPE_ANNOTATION = "exceptionType";
  public static final String EXCEPTION_MESSAGE_ANNOTATION = "exceptionMessage";

  public static final String COPY_TRACE_NAME_PREFIX = "copy: ";
}
