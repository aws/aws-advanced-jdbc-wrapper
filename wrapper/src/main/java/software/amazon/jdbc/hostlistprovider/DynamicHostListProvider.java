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

package software.amazon.jdbc.hostlistprovider;

import software.amazon.jdbc.HostListProvider;

// A marker interface for providers that can fetch a host list, and it changes depending on database status
// A good example of such provider would be DB cluster provider (Aurora DB clusters, patroni DB clusters, etc.)
// where cluster topology (nodes, their roles, their statuses) changes over time.
public interface DynamicHostListProvider extends HostListProvider { }
