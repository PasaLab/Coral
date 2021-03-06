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

Please follow the installation guides to install the data engines correctly. Make sure you can that you can access all data engines through JDBC connections.
- MemSQL: https://www.digitalocean.com/community/tutorials/how-to-install-memsql-on-ubuntu-14-04
- ClickHouse: https://clickhouse.tech/docs/en/getting-started/install/
- PostgreSQL: https://www.postgresqltutorial.com/install-postgresql-linux/

In our experimental environment, the JDBC ports of MemSQL, ClickHouse, and PostgreSQL are 3309, 8123 and 5432, respectively.

# Quick Start

1. Clone source code

   ```
   git clone https://github.com/PasaLab/Coral.git
   ```

2. Package the deep learning libary (DL4J)

   ```
   mvn clean install -Dcheckstyle.skip -DskipTests 
   ```

3. Build Coral

   ```
   mvn clean install -Dcheckstyle.skip -DskipTests -P coral
   ```

4. Start Coral

   ```
   java -cp coral.jar:dl4j.jar \
   -Dcoral.spark.master.node=<spark master> \
   -Dcoral.spark.jar.path=<path to coarl.jar> \
   -Dcoral.spark.executor.memory=15g \
   -Dcoral.conf.dir=<path to coral config directory> \
   -Dsql.logs.dir=<path to log directory> \
   -Dsql.logs.type=A2 \
   zhangyi.adapter.coral.CoralStarter
   ```

5. Connect to the data engines

   ```
   sqlline> !connect jdbc:calcite:model=model.json;lex=JAVA;isCacheEnable=true admin admin
   ```

6. Run queries in the sqlline command line

# Configuration

Coral use the configuration in model.json to connect to the data engines. An example of model.json is  is given below.

```
{
  "version": "1.0",
  "defaultSchema": "clickhouse",
  "schemas": [
    {
      "name": "postgres",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory",
      "operand": {
        "jdbcUrl": "jdbc:postgresql://manager-node:5432/tpc_10",
        "jdbcUser": "postgres",
        "jdbcPassword": ""
      }
    },
    {
      "name": "clickhouse",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory",
      "operand": {
        "jdbcUrl": "jdbc:clickhouse://manager-node:8123",
        "jdbcUser": "default",
        "jdbcPassword": "",
        "jdbcSchema": "tpch_10"
      }
    },
    {
      "name": "memsql",
      "type": "custom",
      "factory": "org.apache.calcite.adapter.jdbc.JdbcSchema$Factory",
      "operand": {
        "jdbcUrl": "jdbc:mysql://manager-node:3309",
        "jdbcUser": "root",
        "jdbcPassword": "",
        "jdbcCatalog": "tpc_10"
      }
    }
  ]
}
```

# Demo

A demo video of Coral running federated queries is available on this [link](https://www.bilibili.com/video/BV1z64y1W7pX/) (video with Chinese subtitles on this [link](https://www.bilibili.com/video/BV1UU4y1t7wb/)).

# Copyright

The code is available for research purpose only.

For commercial usage, please contact PASA Lab@Nanjing University(gurong@nju.edu.cn). 
