# Benchmarks for AWS Advanced JDBC Wrapper

This directory contains a set of benchmarks for the AWS Advanced JDBC Wrapper.
These benchmarks measure the overhead from executing JBDC method calls with multiple connection plugins enabled.
The benchmarks do not measure the performance of target JDBC drivers nor the performance of the failover process.

## Usage
1. Build the benchmarks with the following command `../gradlew jmhJar`.
    1. the JAR file will be outputted to `build/libs`
2. Run the benchmarks with the following command `java -jar build/libs/benchmarks-2.5.6-jmh.jar`.
    1. you may have to update the command based on the exact version of the produced JAR file
