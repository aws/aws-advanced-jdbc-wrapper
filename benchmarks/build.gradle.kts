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

plugins {
    id("me.champeau.jmh")
}

dependencies {
    jmhImplementation(project(":aws-advanced-jdbc-wrapper"))
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.mysql:mysql-connector-j:8.3.0")
    implementation("org.mariadb.jdbc:mariadb-java-client:3.4.1")
    implementation("com.zaxxer:HikariCP:6.2.1")

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.11.3")
    testImplementation("org.mockito:mockito-inline:4.11.0") // 4.11.0 is the last version compatible with Java 8
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
