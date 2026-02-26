#!/usr/bin/env python3
"""
Connection String Generator for AWS Advanced JDBC Wrapper

This module provides functionality to generate JDBC connection strings for different database types
with AWS Advanced JDBC Wrapper configuration.
"""

import logging

logger = logging.getLogger("connection_generator")

class ConnectionStringGenerator:
    """Generates JDBC connection strings for the AWS Advanced JDBC Wrapper."""
    
    def __init__(self):
        self.default_ports = {
            "postgresql": 5432,
            "mysql": 3306,
            "mariadb": 3306,
            "aurora-postgresql": 5432,
            "aurora-mysql": 3306
        }
        
        self.driver_classes = {
            "postgresql": "org.postgresql.Driver",
            "mysql": "com.mysql.cj.jdbc.Driver",
            "mariadb": "org.mariadb.jdbc.Driver",
            "aurora-postgresql": "org.postgresql.Driver",
            "aurora-mysql": "com.mysql.cj.jdbc.Driver"
        }
        
        self.url_prefixes = {
            "postgresql": "jdbc:aws-wrapper:postgresql://",
            "mysql": "jdbc:aws-wrapper:mysql://",
            "mariadb": "jdbc:aws-wrapper:mariadb://",
            "aurora-postgresql": "jdbc:aws-wrapper:postgresql://",
            "aurora-mysql": "jdbc:aws-wrapper:mysql://"
        }
        
        self.plugin_params = {
            "failover": "failover",
            "failover2": "failover2",
            "iam": "iam",
            "secretsmanager": "secretsmanager",
            "hostmonitoring": "hostmonitoring",
            "readwritesplitting": "readwritesplitting",
            "drivermetadata": "drivermetadata",
            "telemetry": "telemetry",
            "initialauroraconnectionstrategy": "initialauroraconnectionstrategy"
        }
    
    def generate(self, database_type, host, database, port=None, plugins=None, additional_params=None):
        """
        Generate a JDBC connection string for the specified database type.
        
        Args:
            database_type (str): The type of database (postgresql, mysql, mariadb, aurora-postgresql, aurora-mysql)
            host (str): The database host
            database (str): The database name
            port (int, optional): The database port. If not provided, the default port for the database type is used.
            plugins (list, optional): List of plugins to enable
            additional_params (dict, optional): Additional connection parameters
            
        Returns:
            dict: A dictionary containing the connection string, driver class, and example code
        """
        if database_type not in self.url_prefixes:
            return {
                "error": f"Unsupported database type: {database_type}",
                "supported_types": list(self.url_prefixes.keys())
            }
        
        # Use default port if not provided
        if port is None:
            port = self.default_ports.get(database_type, 5432)
        
        # Build the base URL
        url = f"{self.url_prefixes[database_type]}{host}:{port}/{database}"
        
        # Add plugin parameters
        params = []
        if plugins:
            plugin_list = []
            for plugin in plugins:
                if plugin in self.plugin_params:
                    plugin_list.append(self.plugin_params[plugin])
            
            if plugin_list:
                params.append(f"wrapperPlugins={','.join(plugin_list)}")
        
        # Add dialect parameter based on database type
        if database_type.startswith("postgresql") or database_type == "aurora-postgresql":
            params.append("wrapperDialect=postgresql")
        elif database_type.startswith("mysql") or database_type == "aurora-mysql" or database_type == "mariadb":
            params.append("wrapperDialect=mysql")
        
        # Add additional parameters
        if additional_params:
            for key, value in additional_params.items():
                params.append(f"{key}={value}")
        
        # Append parameters to URL
        if params:
            url += "?" + "&".join(params)
        
        driver_class = self.driver_classes.get(database_type)
        
        # Generate example code
        example_code = self._generate_example_code(url, driver_class)
        
        return {
            "connection_string": url,
            "driver_class": driver_class,
            "example_code": example_code
        }
    
    def _generate_example_code(self, url, driver_class):
        """Generate example code for using the connection string."""
        return {
            "java": f"""
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class ConnectionExample {{
    public static void main(String[] args) {{
        try {{
            // Register the driver
            Class.forName("{driver_class}");
            
            // Connection properties
            String url = "{url}";
            String user = "your_username";
            String password = "your_password";
            
            // Establish the connection
            Connection connection = DriverManager.getConnection(url, user, password);
            System.out.println("Connection established successfully!");
            
            // Use the connection...
            
            // Close the connection when done
            connection.close();
        }} catch (ClassNotFoundException | SQLException e) {{
            e.printStackTrace();
        }}
    }}
}}
""",
            "datasource": f"""
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import software.amazon.jdbc.ds.AwsWrapperDataSource;

public class DataSourceExample {{
    public static void main(String[] args) {{
        // Create the AWS Wrapper DataSource
        AwsWrapperDataSource dataSource = new AwsWrapperDataSource();
        
        // Set the underlying driver
        dataSource.setDriverClassName("{driver_class}");
        
        // Set connection properties
        dataSource.setUrl("{url}");
        dataSource.setUser("your_username");
        dataSource.setPassword("your_password");
        
        // Use with HikariCP (optional)
        HikariConfig config = new HikariConfig();
        config.setDataSource(dataSource);
        HikariDataSource hikariDataSource = new HikariDataSource(config);
        
        // Use the data source...
    }}
}}
"""
        }
