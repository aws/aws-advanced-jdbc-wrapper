# The JDBC Wrapper Integration with 3rd Party Frameworks

## Hibernate

If you are using [Hibernate](https://hibernate.org/orm/), you can configure database access in the `hibernate.cfg.xml` XML configuration file. If you are using a connection pooler with Hibernate, please review [the Hibernate documentation](https://docs.jboss.org/hibernate/orm/current/quickstart/html_single/#hibernate-gsg-tutorial-basic-config) for configuration information.

```hibernate.cfg.xml
<?xml version="1.0" encoding="utf-8"?>
<!DOCTYPE hibernate-configuration PUBLIC "-//Hibernate/Hibernate Configuration DTD 3.0//EN" "http://hibernate.sourceforge.net/hibernate-configuration-3.0.dtd">
<hibernate-configuration>
    <session-factory>
        <property name="hibernate.connection.driver_class">software.aws.jdbc.Driver</property>
        <property name="hibernate.connection.url">aws-jdbc-wrapper:postgresql://localhost/mydatabase</property>
        <property name="hibernate.connection.username">myuser</property>
        <property name="hibernate.connection.password">secret</property>
    </session-factory>
</hibernate-configuration>
```

## Spring Framework

If you are using Spring, you can use the following sample code as a reference to configure DB access in your application. For more information about Spring, [visit the project website](https://spring.io/).

```SpringJdbcConfig.java
@Configuration
@ComponentScan("com.myapp")
public class SpringJdbcConfig {
    @Bean
    public DataSource mysqlDataSource() {
        DriverManagerDataSource dataSource = new DriverManagerDataSource();
        dataSource.setDriverClassName("software.aws.jdbc.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5432/testDatabase");
        dataSource.setUsername("guest_user");
        dataSource.setPassword("guest_password");

        return dataSource;
    }
}
```