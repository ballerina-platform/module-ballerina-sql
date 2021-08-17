Ballerina SQL Library
===================

  [![Build](https://github.com/ballerina-platform/module-ballerina-sql/actions/workflows/build-timestamped-master.yml/badge.svg)](https://github.com/ballerina-platform/module-ballerina-sql/actions/workflows/build-timestamped-master.yml)
  [![GitHub Last Commit](https://img.shields.io/github/last-commit/ballerina-platform/module-ballerina-sql.svg)](https://github.com/ballerina-platform/module-ballerina-sql/commits/master)
  [![Github issues](https://img.shields.io/github/issues/ballerina-platform/ballerina-standard-library/module/sql.svg?label=Open%20Issues)](https://github.com/ballerina-platform/ballerina-standard-library/labels/module%2Fsql)
  [![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
  [![codecov](https://codecov.io/gh/ballerina-platform/module-ballerina-sql/branch/master/graph/badge.svg)](https://codecov.io/gh/ballerina-platform/module-ballerina-sql)

The SQL library is one of the standard library packages of the<a target="_blank" href="https://ballerina.io/"> Ballerina</a> language.

It provides the common interface and functionality to interact with a database. The corresponding database clients can be created by using specific database packages such as MySQL or using the Java Database Connectivity package JDBC.

For more information on the operations supported by the `sql:Client`, which include the below, go to [The SQL Package](https://docs.central.ballerina.io/ballerina/sql/latest).

- Pooling connections
- Querying data
- Inserting data
- Updating data
- Deleting data
- Updating data in batches
- Executing stored procedures
- Closing the client

For example demonstrations of the usage, go to [Ballerina By Examples](https://ballerina.io/learn/by-example/mysql-init-options.html).

## Issues and Projects 

Issues and Projects tabs are disabled for this repository as this is part of the Ballerina Standard Library. To report bugs, request new features, start new discussions, view project boards, etc. please visit Ballerina Standard Library [parent repository](https://github.com/ballerina-platform/ballerina-standard-library). 

This repository only contains the source code for the package.

## Building from the Source

### Setting Up the Prerequisites

1. Download and install Java SE Development Kit (JDK) version 11 (from one of the following locations).
   * [Oracle](https://www.oracle.com/java/technologies/javase-jdk11-downloads.html)
   * [OpenJDK](http://openjdk.java.net/install/index.html)

2. Download and install [Docker](https://www.docker.com/get-started)
   
3. Export Github Personal access token with read package permissions as follows,
        
        export packageUser=<Username>
        export packagePAT=<Personal access token>

### Building the Source

Execute the commands below to build from source.

1. To build the library:
        
        ./gradlew clean build

2. To run the integration tests:

        ./gradlew clean test

3. To build the package without the tests:

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
   query | query-simple-params<br>query-numeric-params<br>query-complex-params<br>query-row

5. To disable some specific test groups:

        ./gradlew clean build -Pdisable-groups=<Comma separated groups/test cases>

6. To debug the tests:

        ./gradlew clean build -Pdebug=<port>
        ./gradlew clean test -Pdebug=<port>

7. To debug the package with Ballerina language:

        ./gradlew clean build -PbalJavaDebug=<port>
        ./gradlew clean test -PbalJavaDebug=<port>

8. Publish ZIP artifact to the local `.m2` repository:
   
        ./gradlew clean build publishToMavenLocal
   
9. Publish the generated artifacts to the local Ballerina central repository:
   
        ./gradlew clean build -PpublishToLocalCentral=true
   
10. Publish the generated artifacts to the Ballerina central repository:
   
        ./gradlew clean build -PpublishToCentral=true

## Contributing to Ballerina

As an open source project, Ballerina welcomes contributions from the community. 

For more information, go to the [contribution guidelines](https://github.com/ballerina-platform/ballerina-lang/blob/master/CONTRIBUTING.md).

## Code of Conduct

All contributors are encouraged to read the [Ballerina Code of Conduct](https://ballerina.io/code-of-conduct).

## Useful Links

* Chat live with us via our [Slack channel](https://ballerina.io/community/slack/).
* Post all technical questions on Stack Overflow with the [#ballerina](https://stackoverflow.com/questions/tagged/ballerina) tag.
