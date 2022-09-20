# Hibernate Example for the AWS advanced wrapper driver

This example provides a custom AuroraPostgreSQLDialect for hibernate 
which over-rides the `HibernateDialect.buildSQLExceptionConversionDelegate()` function
The purpose of this function is to map custom SQL state codes to Exceptions.

In our case we have 4 states which are added by our driver.

### 08001 - Unable to Establish SQL Connection
When the JDBC Wrapper returns this state, the original connection has 
failed, and the JDBC Wrapper tried to failover to a new instance, 
but was unable to. There are various reasons this may happen: no nodes were available, 
a network failure occurred, and so on. 
In this scenario, please wait until the server is up or other problems are solved. 
We turn this into a `FailoverFailedException`

### 08S02 - Communication Link
When the JDBC Wrapper returns this state, the original connection has failed while 
```autocommit``` was set to ```true```, and the JDBC Wrapper successfully 
failed over to another available instance in the cluster. However, any session state configuration 
of the initial connection is now lost. The exception returned will be a `ConnectionStateUnknownException` 
In this scenario, you should:
- Reuse and reconfigure the original connection (e.g., reconfigure session state to be the same as the original connection).
- Repeat that query that was executed when the connection failed, and continue work as desired.

### 08007 - Transaction Resolution Unknown
When the JDBC Wrapper returns this state, the original connection has failed within a 
transaction (while ```autocommit``` was set to ```false```). 
In this scenario, the JDBC Wrapper first attempts to rollback the transaction and 
then fails over to another available instance in the cluster. The exception returned will be 
a `TransactionStateUnknownException`
Note that the rollback might be unsuccessful as the initial connection may be broken at the 
time that the JDBC Wrapper recognizes the problem. Note also that any session state 
configuration of the initial connection is now lost. In this scenario, you should:

- Reuse and reconfigure the original connection (e.g: reconfigure session state to be the same as the original connection).
- Restart the transaction and repeat all queries which were executed during the transaction before the connection failed.
- Repeat that query which was executed when the connection failed and continue work as desired.

