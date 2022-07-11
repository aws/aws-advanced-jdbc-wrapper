/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

plugins {
    id("me.champeau.jmh")
}

dependencies {
    jmhImplementation(project(":aws-jdbc-proxy-driver"))

    testImplementation("org.junit.jupiter:junit-jupiter-api:5.8.2")
    testImplementation("org.mockito:mockito-inline:4.+")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine")
}

tasks.named<Test>("test") {
    useJUnitPlatform()
}
