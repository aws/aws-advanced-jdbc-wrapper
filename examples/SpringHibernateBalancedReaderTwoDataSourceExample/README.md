# Tutorial: Getting Started with the AWS Advanced JDBC Wrapper, Spring Boot and Hibernate for load-balanced write and read-only connections (Two Datasources)

In this tutorial, you will set up a Spring Boot and Hibernate application with the AWS Advanced JDBC Wrapper, and use two datasources to fetch and update data from an Aurora PostgreSQL database. One datasource is configured to provide a writer connection. The other datasource is configured to provide a reader connection to off load a writer node from read-only queries. Both datasources provide pooled connections through AWS Advanced JDBC Wrapper internal connection pool configuration.

> Note: this tutorial was written using the following technologies:
>    - Spring Boot 3.4.4
>    - Hibernate 6.x (via Spring Boot)
>    - AWS Advanced JDBC Wrapper 4.0.1
>    - Postgresql 42.7.10
>    - Gradle 8
>    - Java 17

