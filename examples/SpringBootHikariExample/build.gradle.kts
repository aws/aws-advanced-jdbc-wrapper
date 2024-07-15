/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *:eq
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

plugins {
	id("org.springframework.boot") version "2.7.0"
	id("io.spring.dependency-management") version "1.1.6"
}

dependencies {
	implementation("org.springframework.boot:spring-boot-starter-data-jdbc")
	implementation("org.springframework.boot:spring-boot-starter-web")
	implementation("org.postgresql:postgresql:42.7.3")
    implementation(project(":aws-advanced-jdbc-wrapper"))

}
