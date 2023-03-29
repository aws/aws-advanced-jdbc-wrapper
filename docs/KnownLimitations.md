# Known Limitations

## Amazon RDS Blue/Green Deployments

The AWS JDBC Driver currently does not support Amazon RDS Blue/Green Deployments and should be avoided. Executing a Blue/Green deployment with the driver will disconnect the driver from the database, and it will be unable to re-establish a connection to an available database instance.
