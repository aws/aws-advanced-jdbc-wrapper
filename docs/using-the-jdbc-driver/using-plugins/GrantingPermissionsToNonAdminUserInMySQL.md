# Using Roles in MySQL 8.0 to Grant Privileges to mysql.rds_topology

For customers using the [Blue/Green plugin](UsingTheBlueGreenPlugin.md) for their planned [Blue/Green Deployment](https://docs.aws.amazon.com/whitepapers/latest/blue-green-deployments/introduction.html), every user account on the DB instance/cluster needs to be granted SELECT privileges to the `mysql.rds_topology` metadata table. This creates an extra operational overhead for customers to adopt fast switchovers.

This document uses [MySQL roles](https://dev.mysql.com/doc/refman/8.0/en/roles.html) to reduce complexity for multi-user grant to read from `mysql.rds_topology`.

## Prerequisites

1. First we need to create a role that grants `SELECT` privilege to `mysql.rds_topology`.
    ```bash
    mysql> CREATE ROLE 'rds_topology_role';  
    Query OK, 0 rows affected (0.06 sec)
    
    mysql> GRANT SELECT ON mysql.rds_topology TO 'rds_topology_role';  
    Query OK, 0 rows affected (0.06 sec)
    ```
2. Then create our test application users.
    ```bash
    mysql> CREATE USER 'app1'@'%' IDENTIFIED BY 'Amaz0n1an_';  
    Query OK, 0 rows affected (0.06 sec)
    
    mysql> CREATE USER 'app2'@'%' IDENTIFIED BY 'Amaz0n1an_';  
    Query OK, 0 rows affected (0.07 sec)
    
    mysql> SHOW GRANTS FOR 'app1'@'%';  
    +----------------------------------+  
    | Grants for app1@%                |  
    +----------------------------------+  
    | GRANT USAGE ON *.* TO `app1`@`%` |  
    +----------------------------------+  
    1 row in set (0.06 sec)
    
    mysql> SHOW GRANTS FOR 'app2'@'%';  
    +----------------------------------+  
    | Grants for app2@%                |  
    +----------------------------------+  
    | GRANT USAGE ON *.* TO `app2`@`%` |  
    +----------------------------------+  
    1 row in set (0.07 sec)
    ```

## Activate the Role

### Option 1: [mandatory_roles](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_mandatory_roles) with [SET DEFAULT ROLE](https://dev.mysql.com/doc/refman/8.0/en/set-default-role.html)

This is recommended if there is only a small and static set of user accounts that require the privileges.

| Pros                                                                          | Cons                                                                                  |
|-------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| Assign roles to ALL user accounts by default, activate only for select users. | The list of user accounts to revoke/activate the role becomes a maintenance overhead. |

First modify the cluster parameter group to add the role `rds_topology_role` we created as a global mandatory role.

```bash
mysql> select @@global.mandatory_roles;  
+--------------------------+  
| @@global.mandatory_roles |  
+--------------------------+  
| rds_topology_role |  
+--------------------------+  
1 row in set (0.06 sec)

```

When the application users connect, they will see that the role will be granted but not active.

```bash
mysql> SELECT CURRENT_USER();  
+----------------+  
| CURRENT_USER() |  
+----------------+  
| app1@% |  
+----------------+  
1 row in set (0.07 sec)

mysql> SHOW GRANTS \G  
*************************** 1. row ***************************  
Grants for app1@%: GRANT USAGE ON *.* TO `app1`@`%`  
*************************** 2. row ***************************  
Grants for app1@%: GRANT `rds_topology_role`@`%` TO `app1`@`%`  
2 rows in set (0.06 sec)

mysql> SELECT * FROM mysql.rds_topology;  
ERROR 1142 (42000): SELECT command denied to user 'app1'@'172.44.75.29'   
for table 'rds_topology'
```

To activate the role, we can use `SET DEFAULT ROLE` via the customer admin account, then we should be able to query the
metadata table.

```bash
mysql> SET DEFAULT ROLE 'rds_topology_role' TO 'app1'@'%', 'app2'@'%';  
Query OK, 0 rows affected (0.07 sec)
```

The application users should be able to query the topology metadata at this time.

```bash
mysql> SHOW GRANTS \G  
*************************** 1. row ***************************  
Grants for app1@%: GRANT USAGE ON *.* TO `app1`@`%`  
*************************** 2. row ***************************  
Grants for app1@%: GRANT SELECT ON `mysql`.`rds_topology` TO `app1`@`%`  
*************************** 3. row ***************************  
Grants for app1@%: GRANT `rds_topology_role`@`%` TO `app1`@`%`  
3 rows in set (0.06 sec)

mysql> SELECT * FROM mysql.rds_topology;  
+------------+----------------------------------------------------------------------------+------+------------------------------+-----------+---------+  
| id | endpoint | port | role | status | version |  
+------------+----------------------------------------------------------------------------+------+------------------------------+-----------+---------+  
| 1116047085 | bgd113287-green-adbzs8.cluster-cyfc0ofzobmh.us-east-1-qa.rds.amazonaws.com | 3306 |
BLUE_GREEN_DEPLOYMENT_TARGET | AVAILABLE | 1.0 |  
| 1125403360 | bgd113287.cluster-cyfc0ofzobmh.us-east-1-qa.rds.amazonaws.com | 3306 | BLUE_GREEN_DEPLOYMENT_SOURCE |
AVAILABLE | 1.0 |  
+------------+----------------------------------------------------------------------------+------+------------------------------+-----------+---------+  
2 rows in set (0.06 sec)

```

### Option 2: [mandatory_roles](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_mandatory_roles) with [activate_all_roles_on_login](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_activate_all_roles_on_login)

This is recommended if the list of user accounts that requires access to the topology metadata table is dynamic.

| Pros                                                        | Cons                                                                                                                                                                                                     |
|-------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Assign and activate the role at the same time to ALL users. | Existing users may have assigned but deactivated roles. `activate_all_roles_on_login` will override this behavior and also activate all roles assigned to existing users aside from `rds_topology_role`. |

First modify the cluster parameter group to add the role `rds_topology_role` we created as a global mandatory role and
enable `activate_all_roles_on_login`.

```bash
mysql> SELECT @@global.activate_all_roles_on_login, @@global.mandatory_roles;  
+--------------------------------------+--------------------------+  
| @@global.activate_all_roles_on_login | @@global.mandatory_roles |  
+--------------------------------------+--------------------------+  
| 1 | rds_topology_role |  
+--------------------------------------+--------------------------+  
1 row in set (0.06 sec)
```

After these change, ALL users will be assigned the new role and it will also be active.

```bash
mysql> SHOW GRANTS \G  
*************************** 1. row ***************************  
Grants for app1@%: GRANT USAGE ON *.* TO `app1`@`%`  
*************************** 2. row ***************************  
Grants for app1@%: GRANT SELECT ON `mysql`.`rds_topology` TO `app1`@`%`  
*************************** 3. row ***************************  
Grants for app1@%: GRANT `rds_topology_role`@`%` TO `app1`@`%`  
3 rows in set (0.06 sec)

mysql> SELECT * FROM mysql.rds_topology;  
+------------+----------------------------------------------------------------------------+------+------------------------------+-----------+---------+  
| id | endpoint | port | role | status | version |  
+------------+----------------------------------------------------------------------------+------+------------------------------+-----------+---------+  
| 1116047085 | bgd113287-green-adbzs8.cluster-cyfc0ofzobmh.us-east-1-qa.rds.amazonaws.com | 3306 |
BLUE_GREEN_DEPLOYMENT_TARGET | AVAILABLE | 1.0 |  
| 1125403360 | bgd113287.cluster-cyfc0ofzobmh.us-east-1-qa.rds.amazonaws.com | 3306 | BLUE_GREEN_DEPLOYMENT_SOURCE |
AVAILABLE | 1.0 |  
+------------+----------------------------------------------------------------------------+------+------------------------------+-----------+---------+  
2 rows in set (0.06 sec)Ò
```
