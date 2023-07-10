## Developer Plugin

The Developer Plugin allows developers to inject a prepared exception to a connection, let the connection to raise this exception on the next call and let an application to process it. Some of the exception that the driver may raise is quite hard to get and it's by the nature of such exceptions. For example, network issues may cause the driver to raise various timeout exceptions. However it may require substantial efforts to design and to build a testing environment where such timeout exceptions could be produced with 100% accuracy and 100% guarantee. If test suite can't verify such network outages cases with 100% accuracy it significantly decreases the value of such tests and makes the tests flaky. The Developer Plugins simplifies testing of such scenarios as shown below.

> :warning: The plugin is NOT intended to be used in production environments. It's designed for the purpose of testing.

### Simulate an exception while opening a new connection

In order to raise a test exception while opening a new connection, it's possible to use global `ExceptionSimulatorManager`. Once the exception is raised it's cleared up in `ExceptionSimulatorManager` and another attempt to open a new connection goes normal.

```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "dev");

final SQLException testExceptionToRaise = new SQLException("test");
ExceptionSimulatorManager.raiseExceptionOnNextConnect(testExceptionToRaise);

Connection connection = DriverManager.getConnection("jdbc:aws-wrapper:postgresql://db-host/", properties); // that throws the exception

Connection anotherConnection = DriverManager.getConnection("jdbc:aws-wrapper:postgresql://db-host/", properties); // it goes normal with no exception
```

### Simulate an exception with already opened connection

A new `dev` plugin should be added to connection plugins parameter in order to be able to intercept JDBC calls and raise a test exception when conditions are met. Similar to previous case, the exception is cleared up once it's raised and subsequent JDBC calls should go normal.  


```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "dev");

Connection connection = DriverManager.getConnection("jdbc:aws-wrapper:postgresql://db-host/", properties);

final ConnectionWrapper connectionWrapper = (ConnectionWrapper) connection;
final ExceptionSimulator simulator = connectionWrapper.unwrap(ExceptionSimulator.class);
final RuntimeException testExceptionToRaise = new RuntimeException("test");
simulator.raiseExceptionOnNextCall("Connection.createStatement", testExceptionToRaise);

final Statement statement = connection.createStatement(); // that throws the exception

final Statement anotherStatement = connection.createStatement(); // it goes normal with no exception
```

