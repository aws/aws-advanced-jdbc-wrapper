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

package software.amazon.jdbc.plugin;

import java.time.Instant;

public class TokenInfo {
  private final String token;
  private final Instant expiration;

  public TokenInfo(final String token, final Instant expiration) {
    this.token = token;
    this.expiration = expiration;
  }

  public String getToken() {
    return this.token;
  }

  public Instant getExpiration() {
    return this.expiration;
  }

  public boolean isExpired() {
    return Instant.now().isAfter(this.expiration);
  }
}
