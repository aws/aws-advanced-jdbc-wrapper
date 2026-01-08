## Developer Plugin

> :warning: The plugin is NOT intended to be used in production environments. It's designed for the purpose of testing.

The Developer Plugin allows developers to inject an exception to a connection and to verify how an application handles it. 

Since some exceptions raised by the drivers rarely happen, testing for those might be difficult and require a lot of effort in building a testing environment. Exceptions associated with network outages are a good example of those exceptions. It may require substantial efforts to design and build a testing environment where such timeout exceptions could be produced with 100% accuracy and 100% guarantee. If a test suite can't produce and verify such cases with 100% accuracy it significantly decreases the value of such tests and makes the tests unstable and flaky. The Developer Plugin simplifies testing of such scenarios as shown below.

The `dev` plugin code should be added to the connection plugins parameter in order to be able to intercept JDBC calls and raise a test exception when conditions are met.

Verify plugin compatibility within your driver configuration using the [compatibility guide](../Compatibility.md).

### Plugin Availability
The plugin is available since version 2.2.3.

### Simulate an exception while opening a new connection

The plugin introduces a new class `ExceptionSimulationManager` that will handle how a given exception will be passed to the connection to be tested.

In order to raise a test exception while opening a new connection, first create an instance of the exception to be tested, then use `raiseExceptionOnNextConnect` in `ExceptionSimulationManager` so it will be triggered at next connection attempt.

Once the exception is raised, it will be cleared and will not be raised again. This means that the next opened connection will not raise the exception again.

```
final Properties properties = new Properties();
properties.setProperty(PropertyDefinition.PLUGINS.name, "dev");

final SQLException testExceptionToRaise = new SQLException("test");
ExceptionSimulatorManager.raiseExceptionOnNextConnect(testExceptionToRaise);

Connection connection = DriverManager.getConnection("jdbc:aws-wrapper:postgresql://db-host/", properties); // that throws the exception

Connection anotherConnection = DriverManager.getConnection("jdbc:aws-wrapper:postgresql://db-host/", properties); // it goes normal with no exception
```

### Simulate an exception with already opened connection

It is possible to also simulate an exception thrown in a connection after the connection has been opened.

Similar to previous case, the exception is cleared up once it's raised and subsequent JDBC calls should behave normally.


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

It's possible to use a callback functions to check JDBC call parameters and decide whether to return an exception or not. Check `ExceptionSimulatorManager.setCallback` and `ExceptionSimulator.setCallback` for more details.
