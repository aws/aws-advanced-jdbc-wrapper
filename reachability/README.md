# Reachability metadata for the AWS Advanced JDBC Wrapper

This directory contains [reachability metadata](https://www.graalvm.org/jdk21/reference-manual/native-image/metadata/) for the AWS Advanced JDBC Wrapper. The metadata can be used to build a [native image](https://www.graalvm.org/jdk21/reference-manual/native-image/) using GraalVM. Note that the `reflect-config.json` file may need to be slightly modified based on your configuration for the AWS Advanced JDBC Wrapper. The instructions to modify the file are below.

## Usage
1. Copy the `.json` files in the relevant version directory to your `src/main/resources/META-INF/native-image` directory.
2. Modify the `reflect-config.json` file as follows:
   1. If you are using `AwsWrapperDataSource`, add a json block defining the target data source class you are using. For example, if you call `AwsWrapperDataSource#setDataSourceClassName("org.postgresql.ds.PGSimpleDataSource")`, then add the following block:
    ```json
    {
      "name": "org.postgresql.ds.PGSimpleDataSource",
      "allPublicConstructors": true
    }
    ```
3. Compile your native image and continue as normal.
