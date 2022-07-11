/*
 * AWS JDBC Proxy Driver
 * Copyright Amazon.com Inc. or affiliates.
 * See the LICENSE file in the project root for more information.
 */

rootProject.name = "jdbc-proxy-driver"

include(
    "aws-jdbc-proxy-driver",
    "benchmarks"
)

project(":aws-jdbc-proxy-driver").projectDir = file("driver-proxy")

pluginManagement {
    plugins {
        fun String.v() = extra["$this.version"].toString()
        fun PluginDependenciesSpec.idv(id: String, key: String = id) = id(id) version key.v()

        id("com.github.spotbugs") version "5.0.+"
        id("com.diffplug.spotless") version "6.8.+"
        id("com.github.vlsi.gradle-extensions") version "1.+"
        id("com.github.vlsi.stage-vote-release") version "1.+"
        id("com.github.vlsi.ide") version "1.+"
        id("me.champeau.jmh") version "0.6.+"
        id("org.checkerframework") version "0.+"
    }
}
