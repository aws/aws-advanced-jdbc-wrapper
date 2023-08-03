# Known Limitations

## Amazon RDS Blue/Green Deployments

This driver currently does not support switchover in Amazon RDS Blue/Green Deployments. In order to execute a Blue/Green deployment with the driver, please ensure your application is coded to retry the database connection. Retry will allow the driver to re-establish a connection to an available database instance. Without a retry, the driver would not be able to identify an available database instance, after a switchover has happened between the blue and green environments. 