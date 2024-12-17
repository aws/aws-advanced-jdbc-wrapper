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

dependencies {
    implementation("org.springframework.boot:spring-boot-starter-jdbc:2.7.13") // 2.7.13 is the last version compatible with Java 8
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("com.mysql:mysql-connector-j:8.3.0")
    implementation("software.amazon.awssdk:rds:2.29.34")
    implementation("software.amazon.awssdk:secretsmanager:2.29.34")
    implementation("software.amazon.awssdk:sts:2.29.34")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.17.1")
    implementation(project(":aws-advanced-jdbc-wrapper"))
    implementation("io.opentelemetry:opentelemetry-api:1.42.1")
    implementation("io.opentelemetry:opentelemetry-sdk:1.42.1")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:1.44.1")
    implementation("com.amazonaws:aws-xray-recorder-sdk-core:2.18.2")
    implementation("org.jsoup:jsoup:1.18.1")
}
