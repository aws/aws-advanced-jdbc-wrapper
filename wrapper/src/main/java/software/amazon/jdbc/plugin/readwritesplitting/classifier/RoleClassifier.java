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

package software.amazon.jdbc.plugin.readwritesplitting.classifier;

import software.amazon.jdbc.HostSpec;

/**
 * Classifies a {@link HostSpec} as a writer or reader ("what role is this host?"). This is the
 * variation point behind the legacy {@code isWriter}/{@code isReader} abstract methods: topology
 * uses the host's {@link software.amazon.jdbc.HostRole}, endpoint mode matches the configured
 * endpoint strings.
 */
public interface RoleClassifier {

  boolean isWriter(HostSpec host);

  boolean isReader(HostSpec host);
}
