# Metadata retrieval support

_Owners_: @kaneeldias  
_Reviewers_: @daneshk @niveathika
_Created_: 2022/06/07  
_Updated_: 2022/06/07
_Issues_: [#2965](https://github.com/ballerina-platform/ballerina-standard-library/issues/2965)

## Summary
Introduce a metadata client, APIs and corresponding record types to retrieve and represent metadata relating to tables,
columns and constraints of relational databases.

## History
In 1.4.x versions and below of SQL packages, there was no specific support to retrieve metadata. Instead, the SQL
`query()` method  could have been used to query the tables specific to the RDBMS containing metadata, and then retrieve
and parse the required information.

This requires the user to have in-depth knowledge of the RDBMS being used in order to understand the schema which
is used to store the metadata.

## Goals
- Support to retrieve and map metadata relating to tables, columns and constraints in a relational database.
- Provide an extensible design which can be implemented by the SQL connectors (`MySQL`, `MSSQL`, `PostgreSQL`, `OracleDB`, and any other future connectors).

## Non-goals
- This would only provide support to retrieve metadata from one database per client. If there is a requirement to retrieve metadata regarding multiple databases, then multiple databases would have to be created.

## Motivation
With this support, the user would not need to write specific queries, nor have in-depth knowledge of the RDBMS being used.
Instead, they may make simple API calls to retrieve the necessary information.

## Description

### Schema client
A new client would be introduced which is used to query the database to retrieve the relevant metadata. On initialization,
the user would have to provide credentials to access the relevant metadata tables (e.g. `INFORMATION_SCHEMA` tables in
MySQL and SQL server). If the provided credentials do not provide the necessary privileges to access the metadata tables,
an `AuthorizationError` would be returned.

```ballerina
# Represents an error that occurs when the user does not have the required authorization to execute an action.
public type InsufficientPrivilegesError distinct ApplicationError;
```

It is also required to provide the name of the database regarding which the metadata should be retrieved.

#### Example client initialization (for MySQL)
```ballerina
# Represents a Mock database schema client.
isolated client class MockSchemaClient {
    *SchemaClient;

    public function init(string host, int port, string user, string password, string database) returns Error? {
        // Use the connector's underlying `client` to establish the connection.
    }
}
```

> **_NOTE:_**  The parameter `database` in the example above refers to the database for which the metadata retrieved is
> about. It does not indicate that the client connects to that database.

This client can be extended by each SQL connector to customize the implementation as necessary.

### Types
New record-types would be introduced to represent the metadata which may be retrieved.
- `TableDefinition`
- `ColumnDefinition`
- `CheckConstraintDefinition`
- `ReferentialConstraintDefinition`
- `RoutineDefinition`
- `ParameterDefinition`

These record types contain only fields which are common to relational databases. However, they may be inherited and then
extended by each SQL connector to provide support for additional database-specific fields.

#### Table Definition
```ballerina
# Represents a table in the database.
#
# + name - The name of the table
# + 'type - Whether the table is a base table or a view
# + columns - The columns included in the table
public type TableDefinition record {
    string name;
    TableType 'type;
    ColumnDefinition[] columns?;
};
```

The `columns` field is optional as there are use cases where the user would not want to retrieve information regarding
the columns of a table.

The `TableType` type is an enum, which can take one of two values:
- `BASE_TABLE`
- `VIEW`

#### Column Definition
```ballerina
# Represents a column in a table.
#
# + name - The name of the column  
# + 'type - The SQL data-type associated with the column
# + defaultValue - The default value of the column  
# + nullable - Whether the column is nullable  
# + referentialConstraints - Referential constraints (foreign key relationships) associated with the column
# + checkConstraints - Check constraints associated with the column
public type ColumnDefinition record {
    string name;
    string 'type;
    anydata? defaultValue;
    boolean nullable;
    ReferentialConstraint[] referentialConstraints?;
    CheckConstraint[] checkConstraints?;
};
```

The `referentialConstraints` and `checkConstraints` fields are optional as there may be use cases where the user does not
want to retrieve this information.

#### Referential Constraint
```ballerina
# Represents a referential constraint (foriegn key constraint).
# 
# + name - The name of the constraint
# + tableName - The name of the table which contains the referenced column
# + columnName - The name of the referenced column
# + updateRule - The action taken when an update statement violates the constraint
# + deleteRule - The action taken when a delete statement violates the constraint
public type ReferentialConstraint record {
    string name;
    string tableName;
    string columnName;
    ReferentialRule updateRule;
    ReferentialRule deleteRule; 
};
```

The `ReferentialRule` type is an enum with four possible values:
- `NO_ACTION`
- `CASCADE`
- `SET_NULL`
- `SET_DEFAULT`

#### Check Constraint
```ballerina
# Represents a check constraint.
# 
# + name - The name of the constraint
# + clause - The actual text of the SQL definition statement
public type CheckConstraint record {
    string name;
    string clause;
};
```

#### Routine Definition
```ballerina
# Represents a routine.
# 
# + name - The name of the routine
# + 'type - The type of the routine (procedure or function)
# + returnType - If the routine returns a value, the return data-type. Else ()
# + parameters - The parameters associated with the routine
public type RoutineDefinition record {
    string name;
    RoutineType 'type;
    string? returnType;
    ParameterDefinition[] parameters;
};
```

The `RoutineType` type is an enum which can take one of two values
- `PROCEDURE`
- `FUNCTION`

#### Parameter Definition
```ballerina
# Represents a routine parameter.
# 
# + mode - The mode of the parameter (IN, OUT, INOUT)
# + name - The name of the parameter
# + type - The data-type of the parameter
public type ParameterDefinition record {
    ParameterMode mode;
    string name;
    string type;
};
```

The `ParameterMode` type is an enum which can take one of three values
- `IN`
- `OUT`
- `INOUT`

### Methods
The `SchemaClient` will contain six methods which may be used to retrieve metadata.
- `listTables()`
- `getTableInfo()`
- `listRoutines()`
- `getRoutineInfo()`

All of these methods will be implemented by each SQL connector.

#### Retrieve all tables
```ballerina
remote isolated function listTables() returns string[]|Error;
```
This would fetch the names of all the tables in the database.

#### Retrieve a single table
```ballerina
remote isolated function getTableInfo(string tableName, ColumnRetrievalOptions include = COLUMNS_ONLY) returns TableDefinition|Error;

public enum ColumnRetrievalOptions {
    NO_COLUMNS,
    COLUMNS_ONLY,
    COLUMNS_WITH_CONSTRAINTS
}
```
This would fetch all relevant information from a given table from the database. Based on the option provided for the `include` parameter, relevant column information would also be retrieved.

#### Retrieve all routines
```ballerina
remote isolated function listRoutines() returns string[]|Error;
```
This would fetch the names of all the routines created in the database.

#### Retrieve a single routine
```ballerina
remote isolated function getRoutineInfo(string name) returns RoutineDefinition|Error;
```
This would fetch all relevant information regarding the provided routine (including the parameter data).

## References
[1] https://docs.microsoft.com/en-us/sql/relational-databases/system-information-schema-views/system-information-schema-views-transact-sql?view=sql-server-ver16
[2] https://www.postgresql.org/docs/current/information-schema.html
[3] https://dev.mysql.com/doc/mysql-infoschema-excerpt/8.0/en/information-schema-table-reference.html
[4] https://docs.oracle.com/cd/E19078-01/mysql/mysql-refman-5.0/information-schema.html
