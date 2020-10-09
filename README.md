Ballerina SQL Library
===================

  [![Build](https://github.com/ballerina-platform/module-ballerina-sql/workflows/Build/badge.svg)](https://github.com/ballerina-platform/module-ballerina-sql/actions?query=workflow%3ABuild)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerina-sql.svg)](https://github.com/ballerina-platform/module-ballerina-sql/commits/master)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

The SQL library is one of the standard library modules of the<a target="_blank" href="https://ballerina.io/"> Ballerina</a> language.

It provides the common interface and functionality to interact with a database. The corresponding database clients can be created by using specific database modules such as MySQL or using the Java Database Connectivity module JDBC.

For more information on the operations supported by the `sql:Client`, which include the below, go to [The SQL Module](https://ballerina.io/swan-lake/learn/api-docs/ballerina/sql/).

- Pooling connections
- Querying data
- Inserting data
- Updating data
- Deleting data
- Updating data in batches
- Executing stored procedures
- Closing the client

For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/swan-lake/learn/by-example/mysql-init-options.html).

## Building from the Source

### Prerequisites

1. Download and install Java SE Development Kit (JDK) version 11 (from one of the following locations).
   * [Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)
   * [OpenJDK](http://openjdk.java.net/install/index.html)

2. Download and install [Docker](https://www.docker.com/get-started)
   
3. Export Github Access Tokens with read package permissions as follows,
        
        export packageUser=<Username>
        export packagePAT=<Access token>

### Building the Source

Execute the commands below to build from source.

1. To build the library:
        
        ./gradlew clean build

2. To run the integration tests:

        ./gradlew clean test

3. To build the module without the tests:

        ./gradlew clean build -x test

4. To run only specific tests:

        ./gradlew clean build -Pgroups=<Comma separated groups/test cases>

   **Tip:** The following groups of test cases are available.<br>
   Groups | Test Cases
   ---| ---
   connection | connection
   pool | pool
   transaction | transaction
   execute | execute-basic <br> execute-params
   batch-execute | batch-execute 
   query | query-simple-params<br>query-numeric-params<br>query-complex-params

4. To debug the tests:

        ./gradlew clean build -Pdebug=<port>

## Contributing to Ballerina

As an open source project, Ballerina welcomes contributions from the community. 

You can also check for [open issues](https://github.com/ballerina-platform/module-ballerina-sql/issues) that interest you. We look forward to receiving your contributions.

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of Conduct

All contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful Links

* Discuss about code changes of the Ballerina project in [ballerina-dev@googlegroups.com](mailto:ballerina-dev@googlegroups.com).
* Chat live with us via our [Slack channel](https://ballerina.io/community/slack/).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
