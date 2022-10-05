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

val pgVersion : String by project
val mysqlVersion : String by project
val mariaVersion : String by project
val jupiterVersion : String by project
val mockitoVersion : String by project

dependencies {
    jmhImplementation(project(":aws-advanced-jdbc-driver"))
    implementation("org.postgresql:postgresql:$pgVersion")
    implementation("mysql:mysql-connector-java:$mysqlVersion")
    implementation("org.mariadb.jdbc:mariadb-java-client:$mariaVersion")

    testImplementation("org.junit.jupiter:junit-jupiter-api:$jupiterVersion")
    testImplementation("org.mockito:mockito-inline:$mockitoVersion")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
