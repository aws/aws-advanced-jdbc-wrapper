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
    implementation("org.postgresql:postgresql:42.7.5")
    implementation("com.mysql:mysql-connector-j:9.2.0")
    implementation("software.amazon.awssdk:rds:2.31.12")
    implementation("software.amazon.awssdk:secretsmanager:2.31.12")
    implementation("software.amazon.awssdk:sts:2.30.27")
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.3")
    implementation(project(":aws-advanced-jdbc-wrapper"))
    implementation("io.opentelemetry:opentelemetry-api:1.49.0")
    implementation("io.opentelemetry:opentelemetry-sdk:1.49.0")
    implementation("io.opentelemetry:opentelemetry-exporter-otlp:1.48.0")
    implementation("com.amazonaws:aws-xray-recorder-sdk-core:2.18.2")
    implementation("org.jsoup:jsoup:1.19.1")
    implementation("com.mchange:c3p0:0.10.2")
}

tasks.withType<JavaExec> {
    systemProperty("java.util.logging.config.file", "${project.layout.buildDirectory.get()}/resources/main/logging-example.properties")
}
