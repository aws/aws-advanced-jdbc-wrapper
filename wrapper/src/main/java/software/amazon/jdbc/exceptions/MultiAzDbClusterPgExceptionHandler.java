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

package software.amazon.jdbc.exceptions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class MultiAzDbClusterPgExceptionHandler extends AbstractPgExceptionHandler {

  // The following SQL States for Postgresql are considered as "communication" errors
  private static final List<String> NETWORK_ERRORS = Arrays.asList(
      "28000", // 28000 for access denied during reboot, this should be considered as a temporary failure
      "53", // insufficient resources
      "57P01", // admin shutdown
      "57P02", // crash shutdown
      "57P03", // cannot connect now
      "58", // system error (backend)
      "08", // connection error
      "99", // unexpected error
      "F0", // configuration file error (backend)
      "XX" // internal error (backend)
  );

  private static final List<String> ACCESS_ERRORS = Collections.singletonList("28P01");

  @Override
  public List<String> getNetworkErrors() {
    return NETWORK_ERRORS;
  }

  @Override
  public List<String> getAccessErrors() {
    return ACCESS_ERRORS;
  }
}
