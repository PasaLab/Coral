# Coral
This is the code repository for the federated query optimization paper titled *'Coral: Federated Query Join Order Optimization Based on Deep Reinforcement Learning'*.

This project is built on the dynamic data management framework [Calcite](https://calcite.apache.org/). We implement the federated query system Coral on top of Calcite.

The detailed setup and running methods are the same as an Java application.

# Prerequisites

- Git
- Java 1.8
- Hadoop 2.7.4
- Spark 2.2.4
- Maven 3.5.4
- MemSQL 7.0
- ClickHouse 19.17.3
- PostgreSQL 10.3

When installing hadoop and spark, make sure that you have basic environment variables for them, such as `HADOOP_HOME`, `SPARK_HOME` and `PATH`.

Please follow the installation guides to install the data engines correctly.
- MemSQL: https://www.digitalocean.com/community/tutorials/how-to-install-memsql-on-ubuntu-14-04
- ClickHouse: https://clickhouse.tech/docs/en/getting-started/install/
- PostgreSQL: https://www.postgresqltutorial.com/install-postgresql-linux/

