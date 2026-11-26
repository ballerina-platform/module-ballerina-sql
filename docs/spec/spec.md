# Specification: Ballerina SQL Library

_Owners_: @niveathika @RDPerera @shafreenAnfar  
_Reviewers_:  
_Created_: 2022/01/13  
_Updated_: 2026/11/26  
_Edition_: Swan Lake  

## Introduction

This is the specification for the SQL standard library of the [Ballerina language](https://ballerina.io/), which provides the client contract, query templates, parameter binding, result mapping, connection pooling and errors shared by the Ballerina SQL database connectors.

This specification is the contract of the library. It defines the observable behaviour of the library: the configuration it accepts, how it executes operations and maps values, the errors it returns, what it rejects at compile time, and what a connector can change. A behaviour of the implementation that differs from this specification is a bug in the implementation. A behaviour that this specification does not define is a gap in the specification, and is also reported as a bug. Signatures and per-symbol documentation are part of the [API documentation](https://central.ballerina.io/ballerina/sql/latest), and usage examples are part of [Ballerina by Example](https://ballerina.io/learn/by-example/). Neither is repeated here.

The released versions of the specification can be found under the relevant GitHub tag. Changes to the specification follow the proposal process described in the [library development process](https://github.com/ballerina-platform/ballerina-library/blob/main/docs/library-development-process.md). If you have any feedback or suggestions about the library, start a discussion via a [GitHub issue](https://github.com/ballerina-platform/ballerina-library/issues) or in the [Discord server](https://discord.gg/ballerinalang).

### Notation

The key words MUST, MUST NOT, REQUIRED, SHALL, SHALL NOT, SHOULD, SHOULD NOT, RECOMMENDED, MAY and OPTIONAL in this document are to be interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119) and [RFC 8174](https://www.rfc-editor.org/rfc/rfc8174) when, and only when, they appear in all capitals. Sentences that state behaviour in the present tense are also normative. Examples, notes and the overview are informative. Behaviour marked as _unspecified_ MUST NOT be relied upon, and behaviour marked as _implementation-defined_ is chosen by the implementation of this library or by the connector, as the rule states.

Durations are in seconds unless stated otherwise. SQL types are written as the names of the JDBC types (`java.sql.Types`), e.g. `VARCHAR`. Behaviour that is described as driver-defined is determined by the JDBC driver and the database the connector uses. Types of this library are written with the `sql:` prefix, except in tables, where the prefix can be omitted.

## Contents

- [1. Overview](#1-overview)
- [2. Terminology](#2-terminology)
- [3. Client](#3-client)
    - [3.1. Description](#31-description)
    - [3.2. Operations](#32-operations)
    - [3.3. Lifecycle](#33-lifecycle)
        - [3.3.1. Creation](#331-creation)
        - [3.3.2. Closed state](#332-closed-state)
    - [3.4. Concurrency](#34-concurrency)
    - [3.5. Errors](#35-errors)
    - [3.6. Interactions](#36-interactions)
- [4. Connection pool](#4-connection-pool)
    - [4.1. Description](#41-description)
    - [4.2. Configuration](#42-configuration)
    - [4.3. Configurable variables](#43-configurable-variables)
    - [4.4. Transaction isolation](#44-transaction-isolation)
    - [4.5. Global pool configuration](#45-global-pool-configuration)
    - [4.6. Pool selection](#46-pool-selection)
    - [4.7. Pool creation](#47-pool-creation)
    - [4.8. XA data sources](#48-xa-data-sources)
    - [4.9. Compile-time validation](#49-compile-time-validation)
    - [4.10. Errors](#410-errors)
    - [4.11. Interactions](#411-interactions)
- [5. Parameterized queries](#5-parameterized-queries)
    - [5.1. Description](#51-description)
    - [5.2. Query templates](#52-query-templates)
    - [5.3. SQL text construction](#53-sql-text-construction)
    - [5.4. Query concatenation](#54-query-concatenation)
    - [5.5. Array flattening](#55-array-flattening)
    - [5.6. Errors](#56-errors)
    - [5.7. Interactions](#57-interactions)
- [6. Parameter binding](#6-parameter-binding)
    - [6.1. Description](#61-description)
    - [6.2. Binding procedure](#62-binding-procedure)
    - [6.3. Basic types](#63-basic-types)
    - [6.4. Time types](#64-time-types)
    - [6.5. Arrays](#65-arrays)
    - [6.6. Typed values](#66-typed-values)
    - [6.7. Typed array values](#67-typed-array-values)
    - [6.8. Deprecated array value](#68-deprecated-array-value)
    - [6.9. Errors](#69-errors)
    - [6.10. Interactions](#610-interactions)
- [7. Result mapping](#7-result-mapping)
    - [7.1. Description](#71-description)
    - [7.2. Target record types](#72-target-record-types)
    - [7.3. Column matching](#73-column-matching)
    - [7.4. Grouping columns into record fields](#74-grouping-columns-into-record-fields)
    - [7.5. Building the value](#75-building-the-value)
    - [7.6. Column conversion](#76-column-conversion)
    - [7.7. Unmatched columns](#77-unmatched-columns)
    - [7.8. Errors](#78-errors)
    - [7.9. Interactions](#79-interactions)
- [8. Query](#8-query)
    - [8.1. Description](#81-description)
    - [8.2. Behaviour](#82-behaviour)
    - [8.3. Stream lifecycle](#83-stream-lifecycle)
    - [8.4. Error streams](#84-error-streams)
    - [8.5. Errors](#85-errors)
    - [8.6. Interactions](#86-interactions)
- [9. Query row](#9-query-row)
    - [9.1. Description](#91-description)
    - [9.2. Behaviour](#92-behaviour)
    - [9.3. Target types](#93-target-types)
    - [9.4. Errors](#94-errors)
    - [9.5. Interactions](#95-interactions)
- [10. Execute](#10-execute)
    - [10.1. Description](#101-description)
    - [10.2. Execution result](#102-execution-result)
    - [10.3. Behaviour](#103-behaviour)
    - [10.4. Generated keys](#104-generated-keys)
    - [10.5. Errors](#105-errors)
    - [10.6. Interactions](#106-interactions)
- [11. Batch execute](#11-batch-execute)
    - [11.1. Description](#111-description)
    - [11.2. Behaviour](#112-behaviour)
    - [11.3. Results](#113-results)
    - [11.4. Failure](#114-failure)
    - [11.5. Errors](#115-errors)
    - [11.6. Interactions](#116-interactions)
- [12. Stored procedure calls](#12-stored-procedure-calls)
    - [12.1. Description](#121-description)
    - [12.2. Parameter classification](#122-parameter-classification)
    - [12.3. Call execution](#123-call-execution)
    - [12.4. Procedure call result](#124-procedure-call-result)
        - [12.4.1. Result set streams](#1241-result-set-streams)
        - [12.4.2. Moving to the next result](#1242-moving-to-the-next-result)
        - [12.4.3. Closing](#1243-closing)
    - [12.5. OUT and INOUT parameter registration](#125-out-and-inout-parameter-registration)
    - [12.6. Reading OUT and INOUT parameters](#126-reading-out-and-inout-parameters)
    - [12.7. OUT parameter type mapping](#127-out-parameter-type-mapping)
    - [12.8. Cursor parameters](#128-cursor-parameters)
    - [12.9. Compile-time validation](#129-compile-time-validation)
    - [12.10. Errors](#1210-errors)
    - [12.11. Interactions](#1211-interactions)
- [13. Transactions](#13-transactions)
    - [13.1. Description](#131-description)
    - [13.2. Behaviour](#132-behaviour)
    - [13.3. Errors](#133-errors)
    - [13.4. Interactions](#134-interactions)
- [14. Metadata retrieval](#14-metadata-retrieval)
    - [14.1. Description](#141-description)
    - [14.2. Operations](#142-operations)
    - [14.3. Metadata types](#143-metadata-types)
    - [14.4. Errors](#144-errors)
- [15. Errors](#15-errors)
    - [15.1. Description](#151-description)
    - [15.2. Error types](#152-error-types)
    - [15.3. Mapping](#153-mapping)
- [16. Observability](#16-observability)
    - [16.1. Description](#161-description)
    - [16.2. Behaviour](#162-behaviour)
    - [16.3. Metrics](#163-metrics)
    - [16.4. Tags](#164-tags)
- [17. Connector contract](#17-connector-contract)
    - [17.1. Description](#171-description)
    - [17.2. Connector obligations](#172-connector-obligations)
    - [17.3. Fixed behaviour](#173-fixed-behaviour)
    - [17.4. Override points](#174-override-points)
    - [17.5. Interactions](#175-interactions)
- [18. Security considerations](#18-security-considerations)
- [19. Planned changes](#19-planned-changes)
- [References](#references)

## 1. Overview

The library defines the contract shared by the Ballerina SQL database connectors, and implements most of their behaviour. It does not connect to a database by itself: a connector, such as `ballerinax/java.jdbc`, `ballerinax/mysql`, `ballerinax/mssql`, `ballerinax/postgresql` or `ballerinax/oracledb`, provides a client class that includes `sql:Client`, and delegates to this library.

The library provides:

- The `sql:Client` object type, with the operations `query`, `queryRow`, `execute`, `batchExecute` and `call`, and `close`.
- Query templates: `sql:ParameterizedQuery` and `sql:ParameterizedCallQuery` values, written as backtick templates whose interpolations are sent to the database as parameters, never as SQL text.
- Typed values, such as `sql:VarcharValue`, that bind a parameter with a specific SQL type, and OUT and INOUT parameters for stored procedures.
- Result mapping from result rows to Ballerina records and other types.
- Connection pools, configured with `sql:ConnectionPool` and configurable variables.
- Participation in Ballerina `transaction` statements.
- The `sql:SchemaClient` object type and records for database metadata.
- The error types of all connectors, and connection pool metrics.

A connector's specification defines how its client is initialized, and states only how it differs from the default behaviour specified here. [Connector contract](#17-connector-contract) defines what a connector can change.

The following function reads the albums of an artist, and adds an album:

```ballerina
import ballerina/sql;

type Album record {|
    string id;
    string title;
    decimal price;
|};

function albumsOf(sql:Client db, string artist) returns Album[]|error {
    stream<Album, sql:Error?> albums = db->query(`SELECT id, title, price FROM albums WHERE artist = ${artist}`);
    return from Album album in albums select album;
}

function addAlbum(sql:Client db, Album album, string artist) returns error? {
    _ = check db->execute(`INSERT INTO albums (id, title, price, artist)
                           VALUES (${album.id}, ${album.title}, ${album.price}, ${artist})`);
}
```

## 2. Terminology

| Term | Meaning |
|---|---|
| Connector | A package that provides a client class including `sql:Client`, such as `ballerinax/java.jdbc`. |
| Client | A value of the client class of a connector. |
| Pool | A connection pool. One pool serves one or more clients, as described in [Pool selection](#46-pool-selection). |
| Query template | A value of `sql:ParameterizedQuery` or `sql:ParameterizedCallQuery`. Its `strings` field holds the SQL text fragments and its `insertions` field holds the parameters. |
| Parameter | A member of the `insertions` of a query template. |
| Typed value | An object whose type includes `sql:TypedValue`, such as `sql:VarcharValue`. It fixes the SQL type with which its value is bound. |
| OUT parameter | An object of a class whose name ends with `OutParameter`, except `InOutParameter`, used in a `sql:ParameterizedCallQuery` to receive a value from a stored procedure. `sql:CursorOutParameter` is an OUT parameter. |
| INOUT parameter | An `sql:InOutParameter` object. It passes a value to a stored procedure and receives one. |
| Target type | The type given by the `typedesc` argument of `query`, `queryRow`, `call`, or of the `get` method of an OUT or INOUT parameter. |
| Database error | A failure reported by the database driver or the database, such as a syntax error, a constraint violation or a lost connection. It results in a `sql:DatabaseError`, or a `sql:BatchExecuteError` for `batchExecute`. |
| Default behaviour | Behaviour that the library implements and that a connector MAY change: the behaviour of the rules and rows referenced in the *Default specified in* column of [Override points](#174-override-points). All other behaviour is fixed ([§17.3](#173-fixed-behaviour)). |
| Configurable variable | A module-level `configurable` variable of the library, set under `[ballerina.sql]` in `Config.toml` or with a `-Cballerina.sql.<name>=<value>` argument. |
| In a transaction | An operation is in a transaction when the condition of rule 1 of [§13.2](#132-behaviour) holds. An operation that is not in a transaction is outside a transaction. |

## 3. Client

### 3.1. Description

`sql:Client` is the object type that every connector's client class includes. It declares the database operations and the `close` method. The `sql` library defines the type and the behaviour behind each operation; a connector provides the class, its initialization, and the driver.

### 3.2. Operations

The `sql:Client` object type is an `isolated client object` type with the following members.

| Member | Kind | Specified in |
|---|---|---|
| `query` | remote method | [§8](#8-query) |
| `queryRow` | remote method | [§9](#9-query-row) |
| `execute` | remote method | [§10](#10-execute) |
| `batchExecute` | remote method | [§11](#11-batch-execute) |
| `call` | remote method | [§12](#12-stored-procedure-calls) |
| `close` | method | [§3.3](#33-lifecycle) |

### 3.3. Lifecycle

#### 3.3.1. Creation

A client is created by the connector's class initializer. The initializer passes a URL, an optional user and password, the connector's driver options, an optional data source class name and the pool configuration to the `sql` library, which performs the following steps.

1. The library selects the pool configuration and the pool of the client as specified in [§4.6](#46-pool-selection). If an active pool is selected, go to step 4.
2. Otherwise the library creates a new pool as specified in [§4.7](#47-pool-creation). If pool creation fails, the initializer returns the `sql:Error` given there, and no client is created.
3. The library obtains one connection from the new pool and returns it to the pool, to verify that the database is reachable. If no connection can be obtained, the initializer returns an `sql:DatabaseError`.
4. The client is registered as a user of the pool, and it is in the open state.

A client that reuses an active pool (step 1) does not open or verify a connection during initialization.

#### 3.3.2. Closed state

1. The `close` method puts the client in the closed state and returns `()`.
2. In the closed state, the `queryRow`, `execute`, `batchExecute` and `call` methods return an `sql:ApplicationError`, and the `query` method returns a stream whose first `next` call returns that error. No database access takes place.
3. Calling `close` on a client that is already closed has no effect and returns `()`.
4. Calling `close` on a client whose initialization did not complete returns `()`.
5. When `close` puts a client in the closed state, the client is removed from the users of its pool. When a pool has no users left, the pool is shut down and all its connections are closed. A later client whose pool selection ([§4.6](#46-pool-selection)) leads to a pool that has been shut down gets a new pool.
6. When `close` puts a client in the closed state, it closes every open stream that the client's `query` method returned, as the stream's `close` method does ([§8.3](#83-stream-lifecycle)); a later `next` on such a stream returns a `sql:ApplicationError`. An `sql:ProcedureCallResult` obtained before `close` is not closed by `close`; whether its results can still be read after the pool is shut down is _unspecified_.
7. The effect of closing a client that has a connection enlisted in a transaction ([§13](#13-transactions)) on that transaction is _unspecified_.

When the program exits, every pool that is still active is shut down.

### 3.4. Concurrency

1. A client can be used from several strands at the same time. Outside a transaction, each operation obtains its own connection from the pool and returns it when the operation's resources are released ([§8](#8-query), [§12](#12-stored-procedure-calls)). To run several operations on one connection, run them in a `transaction` statement ([§13](#13-transactions)).
2. When every connection of the pool is in use, an operation waits for a connection for up to the pool's `connectionTimeout` ([§4.2](#42-configuration)); if none becomes available, the operation returns an `sql:DatabaseError`.
3. In a transaction, the operations of one client use one connection ([§13](#13-transactions)).

### 3.5. Errors

| Condition | Error |
|---|---|
| Any operation on a closed client | `sql:ApplicationError`, returned (for `query`, from the stream's `next`) |
| Pool configuration rejected, driver or data source class not found, or the pool cannot be created | `sql:ApplicationError` returned by the initializer ([§4.10](#410-errors)) |
| The verification connection of a new pool cannot be obtained (step 3 of [§3.3.1](#331-creation)) | `sql:DatabaseError` returned by the initializer |
| No connection available within `connectionTimeout` during an operation | `sql:DatabaseError` returned by the operation |

The message of the closed-client error is `SQL Client is already closed, hence further operations are not allowed`. Other message texts are _unspecified_.

### 3.6. Interactions

- Clients can share a pool ([§4.6](#46-pool-selection)). Closing one client does not affect the other users of its pool.
- A connector can add initialization checks of its own before the steps of [§3.3.1](#331-creation); such checks are specified by the connector ([§17](#17-connector-contract)).

### 3.7. Example: Closed client

```ballerina
import ballerina/sql;

function useAfterClose(sql:Client db) returns boolean {
    sql:Error? closed = db.close();
    sql:ExecutionResult|sql:Error result = db->execute(`DELETE FROM orders`);
    sql:Error? closedAgain = db.close();
    return closed is () && result is sql:ApplicationError && closedAgain is ();
}
```

The function returns `true`: `execute` on a closed client returns an `sql:ApplicationError` without accessing the database (rule 2 of [§3.3.2](#332-closed-state)), and the second `close` returns `()` (rule 3).

## 4. Connection pool

### 4.1. Description

Every client executes its operations on connections taken from a pool. The `sql:ConnectionPool` record configures a pool. Pools are shared between clients according to the pool configuration value and the pool key ([§4.6](#46-pool-selection)).

### 4.2. Configuration

`sql:ConnectionPool` is a closed record. Each field's default is the value of the configurable variable of the same name ([§4.3](#43-configurable-variables)); the defaults below are those that apply when the configurable variable is not set. Time values are in seconds.

| Field | Type | Default | Meaning |
|---|---|---|---|
| `maxOpenConnections` | `int` | `15` | Maximum number of connections in the pool, in use and idle. MUST be at least `1` and at most `2147483647`. |
| `maxConnectionLifeTime` | `decimal` | `1800` | Maximum age of a connection. A connection is retired when it reaches this age and is not in use. `0` means no limit. MUST be `0` or at least `30`. |
| `minIdleConnections` | `int` | `15` | Number of idle connections the pool tries to keep. MUST NOT be negative or greater than `2147483647`. A value greater than the effective `maxOpenConnections` is treated as `maxOpenConnections`. |
| `connectionTimeout` | `decimal` | `30` | Maximum time an operation waits for a connection from the pool. `0` means 2147483647 milliseconds. MUST NOT be negative, and a non-zero value MUST be at least `0.25`. |
| `idleTimeout` | `decimal` | `600` | Time after which an idle connection above `minIdleConnections` is closed. `0` means idle connections are never closed. Has no effect when `minIdleConnections` is not less than `maxOpenConnections`. Adjusted as specified in [§4.7](#47-pool-creation), rule 6. MUST NOT be negative. |
| `validationTimeout` | `decimal` | `5` | Maximum time for checking that a connection is alive. MUST be at least `0.25`. |
| `leakDetectionThreshold` | `decimal` | `0` | Time a connection can be out of the pool before a possible leak is reported in the pool's log. `0` disables leak detection. A value less than `2`, or greater than a non-zero `maxConnectionLifeTime`, also disables it. MUST NOT be negative. |
| `keepAliveTime` | `decimal` | `0` | Interval at which idle connections are checked to keep them alive. `0` disables the check. A value less than `30`, or not less than a non-zero `maxConnectionLifeTime`, also disables it. MUST NOT be negative. |
| `poolName` | `string?` | `()` | Name of the pool, used in its log and in metrics ([§16](#16-observability)). `()` means the pool generates a name; the generated name is _implementation-defined_. |
| `initializationFailTimeout` | `decimal` | `1` | Controls whether pool creation fails when no connection can be opened; see [§4.7](#47-pool-creation), rule 8. Any value is accepted. |
| `transactionIsolation` | `sql:TransactionIsolation?` | `()` | Transaction isolation level set on every connection of the pool. `()` means the driver's default level. |
| `connectionTestQuery` | `string?` | `()` | Query executed to check that a connection is alive. `()` means the driver's connection validity check is used. |
| `connectionInitSql` | `string\|string[]?` | `()` | SQL executed once on every new connection before it is used; see [§4.7](#47-pool-creation), rule 9. `()`, `""` and `[]` mean nothing is executed. |
| `readOnly` | `boolean` | `false` | When `true`, every connection of the pool is put in read-only mode, and write statements fail as specified by the database. |
| `allowPoolSuspension` | `boolean` | `false` | _Deprecated._ Has no effect: the library provides no operation that suspends a pool. |
| `isolateInternalQueries` | `boolean` | `false` | When `true`, the queries the pool itself executes (`connectionTestQuery`, `connectionInitSql`) are isolated in their own transaction when auto-commit is off. |

A value that violates a MUST in this table is rejected as specified in [§4.7](#47-pool-creation), and the client initializer returns an `sql:ApplicationError` ([§4.10](#410-errors)).

The default `minIdleConnections` equals the default `maxOpenConnections`, so by default a pool is fixed-size: it keeps its connections open while idle, and `idleTimeout` has no effect. This is deliberate: a fixed-size pool serves a burst of operations without waiting for new connections to be opened. To let idle connections be closed, set `minIdleConnections` below `maxOpenConnections`.

### 4.3. Configurable variables

The `ballerina/sql` module declares one configurable variable for each field of `sql:ConnectionPool`, with the same name, type and default as the field (`connectionInitSql` has type `readonly & (string|string[]?)`). Each is set like any configurable variable of module `ballerina/sql`, for example under the `[ballerina.sql]` table of `Config.toml` or with `-Cballerina.sql.<name>=<value>`.

1. A configurable variable provides the default value of the corresponding `sql:ConnectionPool` field. A field given in a record value takes precedence; a field omitted from a record value takes the configurable variable's value.
2. Configurable variables are read once, at program start. They also form the global pool configuration ([§4.5](#45-global-pool-configuration)).

#### 4.3.1. Example: Configurable variables

```toml
[ballerina.sql]
maxOpenConnections = 25
connectionTimeout = 20.0
connectionInitSql = ["SET SCHEMA PUBLIC"]
transactionIsolation = "TRANSACTION_READ_COMMITTED"
```

With this file, the global pool configuration and every `sql:ConnectionPool` value that omits these fields have `maxOpenConnections` `25`, `connectionTimeout` `20`, the given `connectionInitSql` and `transactionIsolation`, and the defaults of [§4.2](#42-configuration) for the other fields (rule 1 of [§4.3](#43-configurable-variables)).

### 4.4. Transaction isolation

`sql:TransactionIsolation` is an enumeration whose members are the names of the JDBC isolation levels. The value of `transactionIsolation` is applied to each connection of the pool.

| Member | Isolation level |
|---|---|
| `TRANSACTION_READ_UNCOMMITTED` | Read uncommitted |
| `TRANSACTION_READ_COMMITTED` | Read committed |
| `TRANSACTION_REPEATABLE_READ` | Repeatable read |
| `TRANSACTION_SERIALIZABLE` | Serializable |

The behaviour for a database that does not support the requested level is driver-defined.

### 4.5. Global pool configuration

1. At module initialization, the library creates one immutable `sql:ConnectionPool` value whose fields are the configurable variables ([§4.3](#43-configurable-variables)). This value is the global pool configuration.
2. The `getGlobalConnectionPool` function returns the global pool configuration. Every call returns the same immutable value; an attempt to modify it panics.
3. A connector passes the global pool configuration to the library when the user does not give a pool configuration ([§17](#17-connector-contract)).

### 4.6. Pool selection

Pools are grouped by pool configuration value. Each `sql:ConnectionPool` value that is used to create a client owns a set of pools; the global pool configuration owns the set of global pools.

1. The pool configuration of a client is the `sql:ConnectionPool` value given to the connector, or the global pool configuration when none is given.
2. Within the set of pools of that value, the pool key of the client is its URL, its user and password, its data source class name, whether the pool is an XA pool ([§4.8](#48-xa-data-sources)), and its driver options map. Two keys are equal when the URLs are equal strings, the users, the passwords and the data source class names are equal (both absent counts as equal), both or neither are XA pools, and the options maps, as the connector passes them, are equal by Java object equality: both absent, or the same keys with values that are equal as Java objects. An absent options map and an empty one are therefore different keys, and so are `decimal` values that differ only in scale, such as `1.0d` and `1.00d`. Pool properties that a connector derives from its options ([§4.7](#47-pool-creation)) are computed from the options map, so the key covers them.
3. If the set holds an active pool with an equal key, the client uses that pool.
4. Otherwise, the library creates a new pool ([§4.7](#47-pool-creation)) and adds it to the set, replacing a pool with an equal key that has been shut down.
5. A pool's configuration is read when the pool is created. Changes made later to a mutable `sql:ConnectionPool` value do not affect the pools already created from it.
6. A client never executes operations with credentials other than the user and password it was created with. Clients whose users, passwords or data source classes differ never share a pool (rule 2).
7. Client options that are not part of the pool configuration or the pool key, such as a connector's generated-keys setting ([§10](#10-execute)), apply to each client separately, also when clients share a pool.
8. When a value of the driver options map is `()`, a `byte`, or a structured value such as an array, a map or an `xml` value, the client initializer returns an `sql:ApplicationError`; it does not panic.

As a consequence of rules 1–4:

- Clients that do not give a pool configuration share one global pool per pool key.
- Clients given the same `sql:ConnectionPool` value share one pool per pool key.
- A record value written inline in the client's constructor call is a new value, so the client gets a pool of its own.

#### 4.6.1. Example: Pool sharing

```ballerina
import ballerina/sql;
import ballerinax/java.jdbc;

sql:ConnectionPool shared = {maxOpenConnections: 5};

function createClients(string url) returns error? {
    jdbc:Client a = check new (url, "app", "pw");
    jdbc:Client b = check new (url, "app", "pw");
    jdbc:Client c = check new (url, "app", "pw", connectionPool = shared);
    jdbc:Client d = check new (url, "app", "pw", connectionPool = shared);
    jdbc:Client e = check new (url, "app", "pw", connectionPool = {maxOpenConnections: 5});
    jdbc:Client f = check new (url, "app", "pw", connectionPool = {maxOpenConnections: 5});
}
```

`a` and `b` share a global pool, `c` and `d` share the pool of `shared`, and `e` and `f` each get a pool of their own, although their configurations are equal (rules 1–4 of [§4.6](#46-pool-selection)). Four pools are created.

### 4.7. Pool creation

When a new pool is needed, the library performs the following steps. A failure in steps 1–7 makes the client initializer return an `sql:ApplicationError`.

1. If the connector gives a data source class name, and the Ballerina transaction manager is enabled, and the class implements `javax.sql.XADataSource`, the pool is an XA pool created as specified in [§4.8](#48-xa-data-sources), and the remaining steps do not apply.
2. Each time field of the pool configuration is converted from seconds to milliseconds by converting the `decimal` value to `float`, multiplying by `1000`, and truncating towards zero; values beyond the range of a 64-bit signed integer saturate.
3. The fields are validated in this order, and the first violation fails pool creation: `maxOpenConnections` less than `1` or greater than `2147483647`; `maxConnectionLifeTime` negative, or greater than `0` and less than `30`; `minIdleConnections` negative or greater than `2147483647`; `connectionTimeout` negative; `idleTimeout` negative; `validationTimeout` negative; `leakDetectionThreshold` negative; `keepAliveTime` negative.
4. After conversion, a non-zero `connectionTimeout` less than 250 milliseconds fails pool creation, and so does a `validationTimeout` less than 250 milliseconds, including `0`.
5. If the driver of the URL cannot be found, or the data source class cannot be loaded, cannot be instantiated or is not a `javax.sql.DataSource`, pool creation fails.
6. The pool then adjusts the configuration: `minIdleConnections` greater than `maxOpenConnections` is set to `maxOpenConnections`; `keepAliveTime` and `leakDetectionThreshold` are disabled in the cases stated in [§4.2](#42-configuration); when `minIdleConnections` is less than `maxOpenConnections`, an `idleTimeout` that is not at least one second less than a non-zero `maxConnectionLifeTime` is set to `0`, and otherwise a non-zero `idleTimeout` less than 10 seconds is set to 600 seconds.
7. The URL, user and password are given to the driver. When the connector gives a data source class name, the URL is also set as the data source property `url` unless the options map has a `url` entry, and the user and password are set as the data source properties `user` and `password`. Each entry of the options map is set as a driver (data source) property. If the driver or the data source class rejects a property or its value, pool creation fails.
8. The pool tries to open its first connection according to `initializationFailTimeout` converted to milliseconds, `t`:
    1. If `t` is greater than `0`, the pool tries to open a connection, retrying once per second until `t` has elapsed. If no connection is opened, pool creation fails.
    2. If `t` is `0`, the pool tries once, and a failure does not fail pool creation.
    3. If `t` is negative, the pool does not try.

    In every case, step 3 of [§3.3.1](#331-creation) then verifies the connection, so a database that cannot be reached still makes the initializer fail, with an `sql:DatabaseError` after waiting up to `connectionTimeout`.
9. Each new connection gets the `transactionIsolation` and `readOnly` settings and, if given, executes `connectionInitSql` once. A `string[]` value is joined with `"; "` and executed as one statement, so its effect depends on whether the database accepts several statements in one execution. When `connectionInitSql` or `connectionTestQuery` fails while the pool opens its first connection, pool creation fails.
10. When metrics are enabled, the pool's metrics are registered ([§16](#16-observability)).

A connector MAY supply pool-level properties of its own, derived from its options ([§17.4](#174-override-points)); they are covered by the pool key (rule 2 of [§4.6](#46-pool-selection)). The time an operation waits for a connection, including the time to open and log in a new connection for it, is bounded by the `connectionTimeout` field of the pool configuration alone; a connector does not derive it from its options. A connector's specification states the other pool properties it derives and their values.

### 4.8. XA data sources

1. An XA pool is created when the condition of step 1 of [§4.7](#47-pool-creation) holds. Only `maxOpenConnections`, when greater than `0`, and `maxConnectionLifeTime`, when greater than `0` (truncated to whole seconds), are applied; the other fields of the pool configuration are ignored, and the validation of steps 3 and 4 of [§4.7](#47-pool-creation) is not performed. The client initializer logs a warning when a field that the XA pool ignores has a value other than its default ([§4.2](#42-configuration)).
2. An XA data source requires the Ballerina transaction manager. When the data source class implements `javax.sql.XADataSource` but the transaction manager is not enabled, no XA pool is created, and the outcome is driver-defined: a regular pool is created only when the driver can provide connections from the class as a `javax.sql.DataSource`, and otherwise the client initializer returns an `sql:ApplicationError` ([§4.7](#47-pool-creation), step 5). When a regular pool is created, in a transaction the connection is enlisted through its XA resource ([§13](#13-transactions)).
3. XA pools have no metrics ([§16](#16-observability)).
4. When an XA pool cannot be created, for example because the data source class has no constructor without arguments or its constructor fails, or because a property of the options map is unknown to the class or has a value that cannot be converted, the client initializer returns an `sql:ApplicationError`, as for a regular pool.

### 4.9. Compile-time validation

The `sql` compiler plugin checks variable declarations, at module and function level, whose declared type is `sql:ConnectionPool` or a union containing it, and whose initializer is a mapping constructor. For each field of the constructor whose value is a numeric literal, optionally preceded by a unary `-`, the plugin checks the literal's value, whatever its form (decimal or hexadecimal) and magnitude:

| Code | Field | Condition | Kind |
|---|---|---|---|
| `SQL_101` | `maxOpenConnections` | value less than `1` | error |
| `SQL_102` | `minIdleConnections` | value less than `0` | error |
| `SQL_103` | `maxConnectionLifeTime` | value less than `0`, or greater than `0` and less than `30` | error |

1. Field values that are not literals, including shorthand fields (`{maxOpenConnections}`) and spread fields (`{...pool}`), are not checked, and the build does not fail because of them. The plugin fails the build only by reporting the diagnostics in this table.
2. The plugin does not check a module whose compilation already has errors.
3. These conditions are the lower bounds of the runtime checks of [§4.7](#47-pool-creation), step 3; the runtime checks, including the upper bounds, still apply to values the plugin does not check.

> **Note:** A connector's compiler plugin can report the same codes for a pool configuration written in its client's constructor call; for example, `ballerinax/java.jdbc` does.

### 4.10. Errors

| Condition | Error |
|---|---|
| A pool configuration field violates [§4.7](#47-pool-creation) steps 3 or 4 | `sql:ApplicationError` returned by the client initializer |
| Driver of the URL not found, or data source class not loadable, not instantiable or not a `javax.sql.DataSource` | `sql:ApplicationError` returned by the client initializer |
| The driver or data source class rejects a property or its value ([§4.7](#47-pool-creation), step 7) | `sql:ApplicationError` returned by the client initializer |
| A driver options value of a type the pool key cannot hold (rule 8 of [§4.6](#46-pool-selection)) | `sql:ApplicationError` returned by the client initializer |
| An XA pool cannot be created (rule 4 of [§4.8](#48-xa-data-sources)) | `sql:ApplicationError` returned by the client initializer |
| The pool cannot open its first connection (step 8.1 of [§4.7](#47-pool-creation)), including wrong credentials and an unreachable database, or `connectionInitSql` or `connectionTestQuery` fails during pool creation | `sql:ApplicationError` returned by the client initializer |
| The verification connection cannot be obtained (step 3 of [§3.3.1](#331-creation)) | `sql:DatabaseError` returned by the client initializer |
| No connection available within `connectionTimeout` during an operation | `sql:DatabaseError` returned by the operation |

A database that cannot be reached at client creation is therefore reported as an `sql:ApplicationError` when `initializationFailTimeout` is greater than `0`, and as an `sql:DatabaseError` when it is `0` or negative.

Error messages are _unspecified_, except that a message produced by a configuration check of step 3 names the field, e.g. `ConnectionPool field 'maxOpenConnections' cannot be less than one.`

### 4.11. Interactions

- In a transaction, a client keeps one connection of its pool until the transaction ends ([§13](#13-transactions)), so clients that share a pool compete for its connections.
- `poolName` determines the `pool_name` tag of the pool's metrics ([§16](#16-observability)).
- The global pool configuration is the value connectors pass when the user gives none ([§17](#17-connector-contract)).

## 5. Parameterized queries

### 5.1. Description

A query template carries the SQL text of an operation as a sequence of string fragments and the parameters to bind between them. The client builds the SQL text sent to the driver from the fragments and binds each parameter to a placeholder, so parameter values never become part of the SQL text.

### 5.2. Query templates

1. A `sql:ParameterizedQuery` is a distinct object type that includes `object:RawTemplate`, with a `strings` field whose value is a read-only `string[]` and an `insertions` field of type `sql:Value[]`. A backtick template whose contextual type is `sql:ParameterizedQuery` constructs one: the literal parts of the template become `strings` and the values of the `${...}` interpolations become `insertions`, in order.
2. A `sql:ParameterizedCallQuery` has the same shape, except that `insertions` is of type `sql:Parameter[]`. `sql:Parameter` is the union of `sql:Value`, `sql:InOutParameter`, `sql:OutParameter` and `sql:CursorOutParameter`. Only the `sql:Client` `call` method accepts it ([§12](#12-stored-procedure-calls)).
3. `sql:Value` is the union of `string`, `int`, `boolean`, `float`, `decimal`, `byte[]`, `xml`, `record {}`, the `time` types `time:Date`, `time:TimeOfDay`, `time:Civil` and `time:Utc`, arrays of the basic and `time` types, typed values (`sql:TypedValue`) and `()`. How each kind of value is bound is specified in [§6](#6-parameter-binding).
4. The type of every interpolation is checked at compile time: an interpolation whose type is not a subtype of `sql:Value` (for `sql:ParameterizedQuery`) or `sql:Parameter` (for `sql:ParameterizedCallQuery`) is a compile-time type error. A `sql:ParameterizedCallQuery` is not a subtype of `sql:ParameterizedQuery`, so it cannot be passed to `query`, `queryRow`, `execute` or `batchExecute`.
5. Interpolations are evaluated when the template is constructed. Later changes to the variables used in the interpolations do not change the query template.
6. `strings` and `insertions` are public fields and MAY be assigned after construction, for example to build the SQL text from a `string` value. The client does not check that `strings` has exactly one more member than `insertions`; see [§5.3](#53-sql-text-construction).

> **Note:** A backtick cannot be written in the literal text of a template, and every `${...}` interpolation is a parameter, so `` `${"`"}` `` binds the string `` "`" `` as a value. A literal backtick, such as one that quotes a MySQL identifier, can be put into the SQL text only through a `string` value: by assigning `strings` (rule 6), or by passing to `sql:queryConcat` ([§5.4](#54-query-concatenation)) a query template whose `strings` was assigned in that way.

### 5.3. SQL text construction

The client builds the SQL text of a query template as follows:

1. The members of `strings` are concatenated in order, and the three characters ` ? ` (space, question mark, space) are inserted between each pair of adjacent members, except that when the preceding member ends with `{`, the two characters `? ` (question mark, space) are inserted. A query template with `n` members in `strings` therefore yields SQL text with `n - 1` placeholders; an empty `strings` yields empty SQL text.
2. The members of `strings` are copied verbatim. A `?` character inside them is not escaped, so the driver interprets it according to its own SQL syntax; a `?` that the driver treats as a placeholder must be bound like any other placeholder.
3. The client prepares the SQL text as a JDBC prepared statement and binds the member of `insertions` at index `i` to placeholder `i + 1`, by default as specified in [§6](#6-parameter-binding). `insertions` is not used to build the SQL text.
4. When the number of placeholders differs from the number of members of `insertions`, the driver reports the mismatch, either when an insertion is bound to a placeholder that does not exist or when the statement is executed with a placeholder left unbound, and the operation returns `sql:DatabaseError`.
5. A parameter is always bound as a value. A parameter in a position where the SQL grammar expects an identifier, such as a table or column name, is a placeholder at that position, and the database rejects the statement with a `sql:DatabaseError`. An identifier that is not known until run time can only be made part of the SQL text, by assigning `strings` (rule 6 of [§5.2](#52-query-templates)); the client does not validate or escape such text ([§18](#18-security-considerations)).

The SQL text, not the parameter values, appears in the message of errors that report a failure of the statement. For example, the template `` `INSERT INTO t VALUES (${1}, ${"x"})` `` yields the SQL text `INSERT INTO t VALUES ( ? , ? )`.

### 5.4. Query concatenation

The `sql:queryConcat` function returns one `sql:ParameterizedQuery` built from the `sql:ParameterizedQuery` arguments, in argument order:

1. With no argument, it returns a new query template whose `strings` and `insertions` are both empty.
2. With one argument, it returns that argument itself, not a copy.
3. With two or more arguments, it returns a new query template in which:
    1. the last member of the `strings` of each argument and the first member of the `strings` of the next argument are joined into one member, so that the text between two arguments is contiguous;
    2. all other members of `strings` are kept, in order;
    3. `insertions` is the concatenation of the `insertions` of the arguments, in argument order; the parameter values themselves are not copied;
    4. an argument whose `strings` is empty contributes nothing, and its `insertions` are discarded. Insertions are discarded only from an argument whose fields were assigned directly (rule 6 of [§5.2](#52-query-templates)); the empty query templates of rule 1 and of rule 1 of [§5.5](#55-array-flattening) have no insertions.
4. The result of rule 3 has exactly one more member in `strings` than in `insertions` whenever every argument has.

For example, `` sql:queryConcat(`A${1}B`, `C${2}`, `${3}D`, ``, `E`) `` has `strings` `["A", "BC", "", "DE"]` and `insertions` `[1, 2, 3]`.

### 5.5. Array flattening

The `sql:arrayFlattenQuery` function returns a `sql:ParameterizedQuery` that binds each member of its `sql:Value[]` argument as a separate parameter, separated by `, ` in the SQL text:

1. For an empty array, it returns a query template whose `strings` and `insertions` are both empty, so it contributes no SQL text to a concatenation.
2. For an array of `n` members, `strings` is `""`, followed by `n - 1` members `", "`, followed by `""`, and `insertions` holds the members of the array in order.
3. Each member is one parameter, whatever its type. A member that is itself an array is bound as one array parameter ([§6](#6-parameter-binding)), not flattened further; a `()` member is bound as SQL `NULL`.

> **Note:** Concatenating `sql:arrayFlattenQuery` of an empty array into an `IN (...)` clause yields `IN ()`, which most databases reject with a `sql:DatabaseError`.

### 5.6. Errors

The functions in this section do not return errors. Errors caused by a query template are reported by the operation that executes it:

| Condition | Error |
|---|---|
| An interpolation is not a `sql:Value` (or `sql:Parameter`) | Compile-time type error |
| The number of placeholders differs from the number of `insertions` | `sql:DatabaseError` (reported by the driver) |
| A parameter is used where the SQL grammar requires an identifier | `sql:DatabaseError` (reported by the database) |
| A parameter cannot be bound | As specified in [§6](#6-parameter-binding) |

### 5.7. Interactions

- `sql:batchExecute` compares the SQL text built by [§5.3](#53-sql-text-construction), so all query templates of a batch must have identical `strings` ([§11](#11-batch-execute)).
- A connector MAY override parameter binding (rule 3 of [§5.3](#53-sql-text-construction)), but not the construction of the SQL text ([§17](#17-connector-contract)).

### 5.8. Example: Building an IN clause

The following illustrates [§5.4](#54-query-concatenation) and [§5.5](#55-array-flattening).

```ballerina
function studentsByName(sql:Client db, int minAge, string[] names) returns stream<record {}, sql:Error?> {
    sql:ParameterizedQuery q = sql:queryConcat(
        `SELECT * FROM students WHERE age > ${minAge} AND name IN (`,
        sql:arrayFlattenQuery(names), `)`);
    return db->query(q);
}
```

With `names` `["Anne", "Bob"]`, `q.strings` is `["SELECT * FROM students WHERE age > ", " AND name IN (", ", ", ")"]`, `q.insertions` is `[minAge, "Anne", "Bob"]`, and the SQL text has three placeholders.

### 5.9. Example: Identifiers

The following illustrates rule 5 of [§5.3](#53-sql-text-construction).

```ballerina
function countAbove(sql:Client db, string tableName, int id) returns int|sql:Error {
    // Fails with sql:DatabaseError: the table name is bound as a value.
    int|sql:Error rejected = db->queryRow(`SELECT COUNT(*) FROM ${tableName}`);
    if rejected is sql:DatabaseError {
        // The identifier becomes part of the SQL text; `id` is still a parameter.
        sql:ParameterizedQuery q = ``;
        q.strings = [string `SELECT COUNT(*) FROM ${tableName} WHERE id > `, ""];
        q.insertions = [id];
        return db->queryRow(q);
    }
    return rejected;
}
```

The first `queryRow` call fails because the SQL text is `SELECT COUNT(*) FROM  ? `. The second builds the SQL text `SELECT COUNT(*) FROM students WHERE id >  ? ` for `tableName` `"students"` and binds `id` to its placeholder.

## 6. Parameter binding

### 6.1. Description

Each parameter of a query template is bound to the placeholder at its position in the JDBC prepared statement built from the template ([§5](#5-parameterized-queries)). This section specifies how each kind of `sql:Value` is bound, which SQL type it is bound as, and how values are converted on the way.

### 6.2. Binding procedure

1. The parameters are bound in the order of `insertions`. The parameter at position *i* (zero-based) of `insertions` is bound to placeholder *i + 1*.
2. For each parameter, the binding rule is selected by the parameter's value, in this order:
    1. `()` is bound as SQL `NULL` with the JDBC type `NULL`.
    2. A value of a basic type, a time type, or an array type is bound as specified in [§6.3](#63-basic-types), [§6.4](#64-time-types) and [§6.5](#65-arrays).
    3. A typed value is bound as specified in [§6.6](#66-typed-values) and [§6.7](#67-typed-array-values), according to its class. By default, a typed value whose class is not listed there is rejected with an `sql:UnsupportedTypeError`; a connector MAY bind its own typed value classes ([§17](#17-connector-contract)).
    4. Any other `record {}` value is, by default, rejected with an `sql:DataError`.
3. If a parameter cannot be bound, binding stops, the statement is not executed, and the operation returns the error of [§6.9](#69-errors).
4. A failure reported by the driver while a value is set is handled as specified in [§6.9](#69-errors), rule 2.

### 6.3. Basic types

By default, a parameter of a basic type is bound as follows. The JDBC type is the type the value is set as; the driver and the database convert it to the type of the target column or expression.

| Ballerina type | Bound as (JDBC type) | Conversion |
|---|---|---|
| `string` | `VARCHAR` | The string value. |
| `int` | `BIGINT` | The 64-bit value. |
| `float` | `DOUBLE` | The 64-bit value. `NaN` and the infinities are passed to the driver unchanged. |
| `decimal` | `NUMERIC` | The exact decimal value. |
| `boolean` | `BOOLEAN` | The boolean value. |
| `byte[]` | `VARBINARY` | The bytes. |
| `xml` | `SQLXML` | The XML serialization of the value. |
| `()` | `NULL` | SQL `NULL`. |
| `record {}` other than a time record | — | Rejected with `sql:DataError` ([§6.2](#62-binding-procedure) rule 2.4). An SQL structured value is bound by `sql:StructValue` ([§6.6](#66-typed-values)). |

### 6.4. Time types

1. A `time:Date` value is bound as `DATE` with the given year, month and day. If the fields do not form a valid date, for example `{year: 2021, month: 2, day: 30}`, the operation returns an `sql:ConversionError`.
2. A `time:TimeOfDay` value is bound as follows:
    1. The `second` field defaults to `0` when absent. Its fractional part is rounded half-up to nanoseconds.
    2. Without `utcOffset`, the value is bound as `TIME` with the given hour, minute and second.
    3. With `utcOffset`, the value is bound as `TIME_WITH_TIMEZONE` with that offset. The `seconds` field of the offset is rounded half-up to whole seconds. If the driver does not support this type, the operation returns an `sql:TypeMismatchError`.
    4. The optional `year`, `month` and `day` fields are ignored.
    5. If the fields are out of range, the operation returns an `sql:ConversionError`.
3. A `time:Civil` value is bound as follows:
    1. The `second` field defaults to `0` when absent. Its fractional part is rounded half-up to nanoseconds.
    2. Without `utcOffset`, the value is bound as `TIMESTAMP` whose date and time fields are the fields of the value. When the target stores a time zone, the zone attached by the driver is driver-defined.
    3. With `utcOffset`, the value is bound as `TIMESTAMP_WITH_TIMEZONE` with that offset. The `seconds` field of the offset is rounded half-up to whole seconds. If the driver does not support this type, the operation returns an `sql:TypeMismatchError`.
    4. The `timeAbbrev`, `which` and `dayOfWeek` fields are ignored.
    5. If the fields do not form a valid date-time, the operation returns an `sql:ConversionError`.
4. A `time:Utc` value is bound as `TIMESTAMP` whose date and time fields are the UTC date and time of the instant, with millisecond precision; digits beyond milliseconds are discarded.
5. A `time:Civil[]` value is bound as an SQL array as specified for `sql:DateTimeArrayValue` in [§6.7](#67-typed-array-values), and a `time:TimeOfDay[]` value as specified for `sql:TimeArrayValue`.

> **Note:** For rule 3.2, drivers commonly attach the JVM default time zone.

### 6.5. Arrays

1. An array parameter is bound as an SQL `ARRAY` created by the driver from an SQL element type name and the converted elements. The element type name is the one in the following table.

    | Ballerina type | Element type name | Element conversion |
    |---|---|---|
    | `string?[]` | `VARCHAR` | The string. |
    | `int?[]` | `INTEGER` | The integer value, passed to the driver unaltered. How the driver handles a value outside the range of `INTEGER` is driver-defined. |
    | `boolean?[]` | `BOOLEAN` | The boolean. |
    | `float?[]` | `FLOAT` | The 64-bit value. |
    | `decimal?[]` | `DECIMAL` | The exact decimal value. |
    | `byte[]?[]` | `BINARY` | The bytes. |

2. A `()` element is bound as an SQL `NULL` element.
3. An empty array is bound as an empty SQL array.
4. The element type name carries no length, precision or scale. How the driver applies its defaults for these to the elements is driver-defined; for example, a driver that gives `DECIMAL` a scale of `0` discards the fractional part of the elements.
5. Arrays of other element types, such as arrays of records, are not members of `sql:Value` and are rejected at compile time.

### 6.6. Typed values

1. A typed value's `value` field holds the value to bind; the class selects the SQL type. The following table lists, for each class, the accepted `value` member types and the default binding. In the *Bound as* column, a qualifier in parentheses names the form in which the value is passed to the driver: (string) a character string, (national string) a national character string, (bytes) a byte array, and (boolean) a boolean. In the *`()` bound as* column, `NULL` (`<JDBC type>`) means SQL `NULL` bound with that JDBC type, and `NULL` with a qualifier means a null passed in that form ((string), (national string), (bytes), (date), (time), (timestamp), (row ID)), whose JDBC type is driver-defined.

    | Class | Accepted `value` | Bound as (JDBC type) | `()` bound as |
    |---|---|---|---|
    | `sql:VarcharValue` | `string` | `VARCHAR` (string) | `NULL` (string) |
    | `sql:CharValue` | `string` | `CHAR` (string) | `NULL` (string) |
    | `sql:NVarcharValue` | `string` | `NVARCHAR` (national string) | `NULL` (national string) |
    | `sql:NCharValue` | `string` | `NCHAR` (national string) | `NULL` (national string) |
    | `sql:TextValue` | `string`, `io:ReadableCharacterChannel` | `VARCHAR` (string); a channel's characters are streamed | `NULL` (string) |
    | `sql:ClobValue` | `string`, `io:ReadableCharacterChannel` | `CLOB`; a string is written into a new CLOB, a channel's characters are streamed | `NULL` (`CLOB`) |
    | `sql:NClobValue` | `string`, `io:ReadableCharacterChannel` | `NCLOB`; a string is written into a new NCLOB, a channel's characters are streamed | `NULL` (`CLOB`) |
    | `sql:SmallIntValue` | `int` | `SMALLINT` | `NULL` (`SMALLINT`) |
    | `sql:IntegerValue` | `int` | `INTEGER` | `NULL` (`INTEGER`) |
    | `sql:BigIntValue` | `int` | `BIGINT` | `NULL` (`BIGINT`) |
    | `sql:NumericValue` | `int`, `float`, `decimal` | `NUMERIC` | `NULL` (`DECIMAL`) |
    | `sql:DecimalValue` | `int`, `decimal` | `DECIMAL` | `NULL` (`DECIMAL`) |
    | `sql:RealValue` | `int`, `float`, `decimal` | `REAL` | `NULL` (`FLOAT`) |
    | `sql:FloatValue` | `int`, `float` | `FLOAT` | `NULL` (`FLOAT`) |
    | `sql:DoubleValue` | `int`, `float`, `decimal` | `DOUBLE` | `NULL` (`DOUBLE`) |
    | `sql:BitValue` | `boolean`, `int` | `BIT` (boolean) | `NULL` (`BOOLEAN`) |
    | `sql:BooleanValue` | `boolean` | `BOOLEAN` | `NULL` (`BOOLEAN`) |
    | `sql:BinaryValue` | `byte[]`, `io:ReadableByteChannel` | `BINARY` (bytes); a channel's bytes are streamed | `NULL` (bytes) |
    | `sql:VarBinaryValue` | `byte[]`, `io:ReadableByteChannel` | `VARBINARY` (bytes); a channel's bytes are streamed | `NULL` (bytes) |
    | `sql:BlobValue` | `byte[]`, `io:ReadableByteChannel` | `BLOB` (bytes); a channel's bytes are streamed | `NULL` (bytes) |
    | `sql:DateValue` | `string`, `time:Date` | `DATE` | `NULL` (date) |
    | `sql:TimeValue` | `string`, `time:TimeOfDay` | `TIME`, or `TIME_WITH_TIMEZONE` | `NULL` (time) |
    | `sql:DateTimeValue` | `string`, `time:Civil` | `TIMESTAMP`, or `TIMESTAMP_WITH_TIMEZONE` | `NULL` (timestamp) |
    | `sql:TimestampValue` | `string`, `time:Utc` | `TIMESTAMP` | `NULL` (timestamp) |
    | `sql:StructValue` | `record {}` | `STRUCT` | `NULL` (`STRUCT`) |
    | `sql:RefValue` | `record {}` | `STRUCT` | `NULL` (`STRUCT`) |
    | `sql:RowValue` | `byte[]` | `ROWID` whose bytes are the value | `NULL` (row ID) |

2. Integer conversions:
    1. `sql:BigIntValue` binds the 64-bit value.
    2. `sql:IntegerValue` and `sql:SmallIntValue` bind the value as a 32-bit and a 16-bit integer respectively. A value outside the range of that integer type results in an `sql:DataError`; the value is never bound with altered digits.
3. Exact numeric conversions (`sql:NumericValue`, `sql:DecimalValue`):
    1. A `decimal` is bound exactly.
    2. An `int` is bound exactly.
    3. A `float` is converted to a decimal rounded to 16 significant digits (IEEE 754 decimal64 context). A `NaN` or infinite `float` results in an `sql:DataError`.
4. Approximate numeric conversions:
    1. `sql:DoubleValue` binds the value as a 64-bit floating-point number; a `decimal` is converted to the nearest 64-bit value.
    2. `sql:FloatValue` binds the value as a 64-bit floating-point number.
    3. `sql:RealValue` binds a `float` as a 64-bit floating-point number, and an `int` or `decimal` converted to the nearest 32-bit floating-point number.
5. Bit and boolean conversions: `sql:BitValue` binds `true`/`false` as given and an `int` `1` or `0` as `true` or `false`. Any other `int` results in an `sql:DataError`.
6. Character channels and byte channels are read by the driver when the statement is executed. A channel is consumed by the binding.
7. String forms of date and time values:
    1. `sql:DateValue` parses a `string` as `yyyy-[m]m-[d]d`. A string not in this form, or whose fields do not form a valid date, results in an `sql:ConversionError`.
    2. `sql:TimeValue`, `sql:DateTimeValue` and `sql:TimestampValue` pass a `string` to the driver as a string, which converts it to the SQL type of the target. The accepted formats are driver-defined; a string the driver rejects results in a database error ([§6.9](#69-errors)).
8. A `time:Date`, `time:TimeOfDay`, `time:Civil` or `time:Utc` value in a typed value is bound as specified in [§6.4](#64-time-types).
9. `sql:StructValue` and `sql:RefValue` create an SQL structured value whose SQL type name is the name of the record's type in upper case, converted independently of the JVM default locale (for example, `item` becomes `ITEM` under every locale), and whose attributes are the record's field values in field declaration order. For a record value of an anonymous type the SQL type name is _unspecified_. Fields that the record type does not declare, such as the rest fields of an open record, are not passed. Each declared field is passed as follows:
    1. A field of type `int`, `float`, `decimal`, `string` or `boolean` is passed unchanged.
    2. A field of type `byte[]` is passed as bytes; a field of any other array type results in an `sql:DataError`.
    3. A field of a record type is passed as a nested structured value created by the same rules.
    4. A field of any other type results in an `sql:DataError`.

### 6.7. Typed array values

1. A typed array value is bound as an SQL `ARRAY` created by the driver from the element type name in the following table and the converted elements. A `()` element is bound as an SQL `NULL` element, and an empty array, which is the default `value` of every typed array class, is bound as an empty SQL array.

    | Class | Accepted `value` | Element type name | Element conversion |
    |---|---|---|---|
    | `sql:VarcharArrayValue` | `string?[]` | `VARCHAR` | The string. |
    | `sql:CharArrayValue` | `string?[]` | `CHAR` | The string. |
    | `sql:NVarcharArrayValue` | `string?[]` | `NVARCHAR` | The string. |
    | `sql:SmallIntArrayValue` | `int?[]` | `SMALLINT` | As `sql:SmallIntValue`. |
    | `sql:IntegerArrayValue` | `int?[]` | `INTEGER` | As `sql:IntegerValue`. |
    | `sql:BigIntArrayValue` | `int?[]` | `BIGINT` | As `sql:BigIntValue`. |
    | `sql:NumericArrayValue` | `int?[]`, `float?[]`, `decimal?[]` | `NUMERIC` | As `sql:NumericValue`. |
    | `sql:DecimalArrayValue` | `int?[]`, `decimal?[]` | `DECIMAL` | As `sql:NumericValue`. |
    | `sql:RealArrayValue` | `int?[]`, `float?[]`, `decimal?[]` | `REAL` | 64-bit floating-point number. |
    | `sql:FloatArrayValue` | `int?[]`, `float?[]` | `FLOAT` | 64-bit floating-point number. |
    | `sql:DoubleArrayValue` | `int?[]`, `float?[]`, `decimal?[]` | `DOUBLE` | 64-bit floating-point number. |
    | `sql:BitArrayValue` | `boolean?[]`, `int?[]` | `BIT` | As `sql:BitValue`. |
    | `sql:BooleanArrayValue` | `boolean?[]` | `BOOLEAN` | The boolean. |
    | `sql:BinaryArrayValue` | `byte[]?[]`, `io:ReadableByteChannel[]` | `BINARY` | The bytes; a channel is read fully into memory. |
    | `sql:VarBinaryArrayValue` | `byte[]?[]`, `io:ReadableByteChannel[]` | `VARBINARY` | The bytes; a channel is read fully into memory. |
    | `sql:DateArrayValue` | `string?[]`, `time:Date?[]` | `DATE` | As `sql:DateValue`. |
    | `sql:TimeArrayValue` | `string?[]`, `time:TimeOfDay?[]` | `TIME`, or `TIME_WITH_TIMEZONE` | See rule 2. |
    | `sql:DateTimeArrayValue` | `string?[]`, `time:Civil?[]` | `TIMESTAMP`, or `TIMESTAMP_WITH_TIMEZONE` | See rule 3. |
    | `sql:TimestampArrayValue` | `string?[]`, `time:Utc?[]` | `TIMESTAMP` | See rule 3. |

2. `sql:TimeArrayValue` converts a `string` element in the form `hh:mm:ss`, without fractional seconds, and a `time:TimeOfDay` element as in [§6.4](#64-time-types). If any element has a `utcOffset`, the element type name is `TIME_WITH_TIMEZONE`, otherwise `TIME`. A string in another form, or out-of-range fields, result in an `sql:ConversionError`.
3. `sql:DateTimeArrayValue` and `sql:TimestampArrayValue` convert a `string` element in the form `yyyy-MM-dd HH:mm:ss`, without fractional seconds, a `time:Civil` element as in [§6.4](#64-time-types), and a `time:Utc` element as the UTC date and time of the instant with millisecond precision. For `sql:DateTimeArrayValue`, if any `time:Civil` element has a `utcOffset`, the element type name is `TIMESTAMP_WITH_TIMEZONE`, otherwise `TIMESTAMP`. A string in another form, or fields that do not form a valid date-time, result in an `sql:ConversionError`.
4. An element value that cannot be converted to the element type results in an `sql:DataError` (for example, an `sql:BitArrayValue` element `2`), or in an `sql:ConversionError` for the date and time conversions of rules 2 and 3.

### 6.8. Deprecated array value

> **Deprecated:** since 0.6.0-beta.1. Use an array (`string?[]`, `int?[]`, …) or a typed array value class.

1. `sql:ArrayValue` accepts `string[]`, `int[]`, `boolean[]`, `float[]`, `decimal[]` and `byte[][]`, and `()`.
2. The value is bound as an SQL `ARRAY` with the element type name `VARCHAR`, `BIGINT`, `BOOLEAN`, `DOUBLE`, `DECIMAL` and `BINARY` respectively; elements are converted without loss.
3. `()` is bound as a `NULL` array, and an empty array as an empty SQL array.

### 6.9. Errors

1. A parameter rejected by the binding rules results in the following errors, returned by the operation:

    | Condition | Error |
    |---|---|
    | A `record {}` parameter that is not a time record | `sql:DataError` |
    | A typed value whose class is not listed in [§6.6](#66-typed-values) or [§6.7](#67-typed-array-values) and is not bound by the connector ([§17.4](#174-override-points)) | `sql:UnsupportedTypeError` |
    | A date or time value whose fields do not form a valid value, or a string form that cannot be parsed ([§6.6](#66-typed-values) rule 7.1, [§6.7](#67-typed-array-values) rules 2 and 3) | `sql:ConversionError` |
    | A `time:TimeOfDay` or `time:Civil` with `utcOffset` when the driver does not support the zoned type | `sql:TypeMismatchError` |
    | An `sql:BitValue` or `sql:BitArrayValue` `int` other than `0` or `1` | `sql:DataError` |
    | A field of an unsupported type in an `sql:StructValue` or `sql:RefValue` record | `sql:DataError` |
    | An integer out of range of an `sql:SmallIntValue` or `sql:IntegerValue` | `sql:DataError` |
    | A `NaN` or infinite `float` in an exact numeric typed value | `sql:DataError` |

2. A database error raised by the driver while a value is set is returned as follows: if its message contains `data exception` or `incompatible data type`, as an `sql:DataError` whose message starts with `Error while constructing SQL query.`; otherwise, as an `sql:DatabaseError` ([§15](#15-errors)). Because the message text is chosen by the driver, whether a given binding failure is an `sql:DataError` or an `sql:DatabaseError` is driver-defined.
3. Binding errors are returned, never raised as panics. The text of the messages, beyond the prefix of rule 2, is _unspecified_.

### 6.10. Interactions

- The binding rules apply to every parameter of `query`, `queryRow`, `execute` and `batchExecute` ([§8](#8-query)–[§11](#11-batch-execute)), and to the IN value of an `sql:InOutParameter` and the parameters of `call` ([§12](#12-stored-procedure-calls)). The SQL type a parameter is bound as is also the type an `sql:InOutParameter` is registered with.
- A connector MAY override the binding of any typed value class, array, time value or `record {}` value ([§17](#17-connector-contract)); the rules of this section are the default behaviour.
- Time zone handling of a value read back from the database is specified in [§7.6](#76-column-conversion). In particular, a `time:Utc` round trip through a column without a time zone is specified in [§7.6](#76-column-conversion) rule 5.

### 6.11. Example: Typed values and nil

```ballerina
function ex6Typed(sql:Client db, int id) returns sql:Error? {
    sql:SmallIntValue age = new (12);
    sql:VarcharValue nickname = new ();
    _ = check db->execute(`UPDATE students SET age = ${age}, nickname = ${nickname} WHERE id = ${id}`);
}
```

`age` is bound as `SMALLINT` `12`, `nickname` as a `NULL` string, and `id` as `BIGINT` ([§6.3](#63-basic-types), [§6.6](#66-typed-values) rule 1).

### 6.12. Example: Time values

```ballerina
function ex6Time(sql:Client db) returns sql:Error? {
    time:Civil local = {year: 2021, month: 2, day: 3, hour: 10, minute: 20, second: 30.5};
    time:Civil offset = {year: 2021, month: 2, day: 3, hour: 10, minute: 20, second: 0, utcOffset: {hours: 5, minutes: 30}};
    sql:DateValue day = new ("2021-02-03");
    _ = check db->execute(`INSERT INTO events (local_ts, zoned_ts, day) VALUES (${local}, ${offset}, ${day})`);
}
```

`local` is bound as `TIMESTAMP` `2021-02-03 10:20:30.5`, `offset` as `TIMESTAMP_WITH_TIMEZONE` `2021-02-03T10:20+05:30`, and `day` as `DATE` `2021-02-03` ([§6.4](#64-time-types) rule 3, [§6.6](#66-typed-values) rule 7.1).

### 6.13. Example: Arrays

```ballerina
function ex6Array(sql:Client db) returns sql:Error? {
    int?[] scores = [90, (), 75];
    sql:VarcharArrayValue tags = new (["a", "b"]);
    _ = check db->execute(`INSERT INTO results (scores, tags) VALUES (${scores}, ${tags})`);
}
```

`scores` is bound as an `INTEGER` array whose second element is `NULL`, and `tags` as a `VARCHAR` array ([§6.5](#65-arrays), [§6.7](#67-typed-array-values)).

## 7. Result mapping

### 7.1. Description

Result mapping converts a row of a result set to a value of the target type: each column is matched to a field of the target record type and its value is converted to the field's type. The per-column conversion of [§7.6](#76-column-conversion) also applies to a non-record target of `queryRow` ([§9](#9-query-row)).

### 7.2. Target record types

1. The target type of a row is a record type. It may be named or anonymous, open or closed.
2. A target type `T & readonly`, where `T` is a record type, is supported: the row is mapped to `T` and the result is read-only. A target type `map<anydata>` is mapped as `record {}`. Any other target type that is not a record type is not supported; the operation returns an `sql:Error`, whose specific type is _unspecified_.
3. `record {}` is the open record type without declared fields: every column becomes a field as specified in [§7.7](#77-unmatched-columns).

### 7.3. Column matching

1. Each column is identified by its label as reported by the driver (the alias given by `AS`, otherwise the column name). Labels are compared as reported.
2. A field's column name is the `name` of its `sql:Column` annotation, if present, otherwise the field name. A field with an `sql:Column` annotation does not match a column through its field name.
3. Before matching, a column is assigned to a record field group as specified in [§7.4](#74-grouping-columns-into-record-fields).
4. A column that is not grouped matches the first field, in field declaration order, whose column name equals the column label ignoring case.
5. If a field is matched by more than one column, the field takes the value of the last of those columns. Because matching ignores case, columns such as `ID` and `id` both match a field `id`; give the columns distinct aliases to read both.
6. A matched column's value is converted to the field's type as specified in [§7.6](#76-column-conversion). Whether the field's type can hold the column's SQL type is checked once, before the first row is converted; if it cannot, the operation returns an `sql:TypeMismatchError`.
7. A column that matches no field is handled as specified in [§7.7](#77-unmatched-columns).
8. The `sql:Column` annotation is honoured on the fields of named and anonymous record types.

> **Note:** Many drivers report unquoted identifiers in upper case, so the label of an unquoted identifier is often its upper-case form.

### 7.4. Grouping columns into record fields

1. Grouping applies only to named record types. For an anonymous record type, no column is grouped.
2. A field is a group field if its type is a record type; a field whose type is a union, such as `Teacher?`, is not a group field. The group name of a group field is the `name` of its `sql:Column` annotation, if present, otherwise the field name.
3. A column's prefix is determined as follows:
    1. If the column label contains a `.`, the prefix is the part before the first `.`, and the inner label is the part after it.
    2. Otherwise, if an earlier column of the result set has the same label, the prefix is the column's table name as reported by the driver, in upper case, and the inner label is the column label.
    3. Otherwise, the column has no prefix.
4. A column whose prefix equals a group name, ignoring case, is grouped into that group field: it is matched against the fields of the group field's record type by its inner label, using the rules of [§7.3](#73-column-matching) and [§7.7](#77-unmatched-columns).
5. Any other column is matched at the top level. A column that has a prefix because of rule 3.2 but is not grouped is matched with the label `<TABLE>.<label>`.
6. A group field into which no column is grouped is treated as a field without a matching column ([§7.5](#75-building-the-value) rule 2).

### 7.5. Building the value

1. A field matched by a column holds the converted column value. An SQL `NULL` converts to `()`:
    1. If the field's type includes `()`, the field holds `()`.
    2. If the field is optional and its type does not include `()`, the field is absent.
    3. Otherwise, the operation returns an `sql:TypeMismatchError`.
2. A field that no column matches is set as follows:
    1. A field with a default value holds its default value.
    2. An optional field is absent.
    3. A required field whose type includes `()` holds `()`. This lets one record type serve queries that do not all select its nilable columns; as a consequence, a misspelled column label or `sql:Column` name is not reported for a nilable field.
    4. A required field whose type does not include `()` and that has no default value cannot be populated: the operation returns an `sql:DataError`, whose specific subtype is _unspecified_.
3. If a value added for an unmatched column ([§7.7](#77-unmatched-columns)) does not belong to the rest field type of the record, the operation returns an `sql:TypeMismatchError`.

### 7.6. Column conversion

1. A column value is converted according to the column's JDBC type and the target type. The target type is the field type, or for a non-record target of `queryRow` the target type itself ([§9](#9-query-row)). If the target type is a union, the member type is selected independently of the order in which the union is written: the default type of the column's JDBC type ([§7.7](#77-unmatched-columns)) when the union contains it; otherwise the single member that the column's JDBC type accepts in the following table; if several members accept it and none is the default type, the operation returns an `sql:TypeMismatchError`. So `string|int` and `int|string` give the same value; `()` members are handled as specified in [§7.5](#75-building-the-value). A union whose members are all singleton or `()` types, such as an `enum` type, is accepted for every JDBC type: the value is converted to the default type of the JDBC type ([§7.7](#77-unmatched-columns)) and must belong to the union, otherwise the operation returns an `sql:TypeMismatchError`.
2. The following table gives, for each JDBC type, the accepted target types and the conversion. A target type not listed for a JDBC type results in an `sql:TypeMismatchError`.

    | JDBC type | Target type | Conversion |
    |---|---|---|
    | `CHAR`, `VARCHAR`, `LONGVARCHAR`, `NCHAR`, `NVARCHAR`, `LONGNVARCHAR` | `string` | The string as returned by the driver (including padding of `CHAR`). |
    | | `json` | The string parsed as JSON. A string that is not valid JSON results in an `sql:ConversionError`. |
    | `CLOB`, `NCLOB` | `string` | The full character content. |
    | | `json` | The character content parsed as JSON, as for `VARCHAR`. |
    | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `int` | The integer value. |
    | | `string` | The decimal representation of the integer. |
    | `BIT`, `BOOLEAN` | `boolean` | The boolean value. |
    | | `int` | `1` for true, `0` for false. |
    | | `string` | `"true"` or `"false"`. |
    | `NUMERIC`, `DECIMAL` | `decimal` | The exact decimal value. |
    | | `int` | The value, when it is an integer within the range of `int`; any other value results in an `sql:TypeMismatchError` for the row. |
    | | `string` | The plain decimal representation, with the column's scale. |
    | `REAL` | `float` | The value read as a 32-bit floating-point number and widened. |
    | | `string` | The representation of the widened value. |
    | `FLOAT`, `DOUBLE` | `float` | The 64-bit value. |
    | | `string` | The representation of the value. |
    | `BINARY`, `VARBINARY`, `LONGVARBINARY` | `byte[]` | The bytes. |
    | | `string` | The string the driver returns for the value (for example, hexadecimal digits). |
    | `BLOB` | `byte[]` | The full content. |
    | | `string` | The full content as text; the character encoding with which the bytes are decoded is _unspecified_. |
    | `ROWID` | `byte[]` | The bytes of the row ID. |
    | `DATE` | `time:Date` | The year, month and day. |
    | | `string` | `yyyy-mm-dd`. |
    | | `int` | See rule 5. |
    | `TIME` | `time:TimeOfDay` | The hour, minute and whole second; fractional seconds are discarded. |
    | | `string` | `hh:mm:ss`; fractional seconds are discarded. |
    | | `int` | See rule 5. |
    | `TIME_WITH_TIMEZONE` | `time:TimeOfDay` | The hour, minute and second with fractional seconds, `utcOffset` set to the offset, and `timeAbbrev` set to the offset in `±hh:mm` form (`Z` for UTC). |
    | | `string` | ISO 8601 local time with offset: `hh:mm`, then `:ss` when the seconds or the fraction are not zero, then the fraction in 3, 6 or 9 digits when it is not zero, then the offset as `±hh:mm`, or `Z` for UTC; for example `10:20:30.500+05:30` or `10:20Z`. |
    | | `int` | _Unspecified_ (see rule 5). |
    | `TIMESTAMP` | `time:Civil` | The date and time fields with fractional seconds; no `utcOffset`. |
    | | `time:Utc` | See rule 5. |
    | | `string` | `yyyy-mm-dd hh:mm:ss.f…`, with one to nine fractional digits, trailing zeros removed, and at least one fractional digit. |
    | | `int` | See rule 5. |
    | `TIMESTAMP_WITH_TIMEZONE` | `time:Civil` | The date and time fields with fractional seconds, `utcOffset` set to the offset, and `timeAbbrev` set to the offset in `±hh:mm` form (`Z` for UTC). |
    | | `time:Utc` | The instant, with millisecond precision. |
    | | `string` | `yyyy-mm-dd`, then `T`, then the time and offset in the form of the `TIME_WITH_TIMEZONE` row; for example `2021-02-03T10:20:30.250+05:30` or `2021-02-03T10:20Z`. |
    | | `int` | Milliseconds from the epoch to the instant. |
    | `ARRAY` | an array type | See rule 3. |
    | | `anydata` | The array of rule 3, as for an unmatched column ([§7.7](#77-unmatched-columns)). |
    | `STRUCT`, `REF` | a record type | See rule 4. |
    | `SQLXML` | `xml` | The XML content parsed as an `xml` value. |
    | any other JDBC type | `string`, `any`, `anydata` | The string the driver returns for the value. |
    | | `int` | The value read as an integer. |
    | | `boolean`, `float`, `decimal`, `byte[]`, `xml`, `json` | The value read as that type. |
    | | a record type | By default, results in an `sql:UnsupportedTypeError`. |

3. An SQL array is converted as follows:
    1. The element type of the result is determined by the class of the elements the driver returns, not by the target type: character elements become `string`, boolean elements `boolean`, integer elements `int`, floating-point elements `float`, exact numeric elements `decimal`, binary elements `byte[]`, date elements `time:Date`, time elements `time:TimeOfDay`, and timestamp elements `time:Civil`; zoned time and timestamp elements carry `utcOffset`.
    2. If any element is `NULL`, the result is an array of the element type and `()`, such as `int?[]`; otherwise it is an array of the element type, such as `int[]`.
    3. An empty SQL array converts to `()` when the target type includes `()`, such as `int[]?` or `anydata`, and otherwise to an empty array of the target array type.
    4. If the result does not belong to the field's type, for example `int[]` into a `string[]` field, or `int?[]` into an `int[]` field, the operation returns an `sql:TypeMismatchError`.
4. An SQL structured value is converted to the target record type by position: its attributes are assigned to the record's fields in field declaration order. If the number of attributes differs from the number of fields, the operation returns an `sql:FieldMismatchError`. A field of an unsupported type results in an `sql:TypeMismatchError`.
5. Conversions involving a time zone:
    1. A `TIMESTAMP` value converted to `time:Utc` or `int`, and a `DATE` or `TIME` value converted to `int`, are interpreted as local date-times in UTC, whatever the JVM default time zone.
    2. A `time:Utc` value bound to a `TIMESTAMP` column ([§6.4](#64-time-types) rule 4) and read back into `time:Utc` yields the same instant truncated to milliseconds.
    3. An `int` converted from `DATE`, `TIME` or `TIMESTAMP` is the number of milliseconds from the epoch to that local date-time; for `TIME`, the date is 1970-01-01.
    4. The `int` conversion of a `TIME_WITH_TIMEZONE` value is _unspecified_.
6. An SQL `NULL` converts to `()` for every JDBC type and target type ([§7.5](#75-building-the-value) rule 1).

### 7.7. Unmatched columns

1. A column that matches no field of a closed record type results in an `sql:FieldMismatchError`.
2. A column that matches no field of an open record type is added to the record as a field named by the column label (for a column renamed by [§7.4](#74-grouping-columns-into-record-fields) rule 5, `<TABLE>.<label>`), with the value converted to the default type of its JDBC type:

    | JDBC type | Default type |
    |---|---|
    | `CHAR`, `VARCHAR`, `LONGVARCHAR`, `NCHAR`, `NVARCHAR`, `LONGNVARCHAR`, `CLOB`, `NCLOB` | `string` |
    | `DATE`, `TIME`, `TIMESTAMP`, `TIME_WITH_TIMEZONE`, `TIMESTAMP_WITH_TIMEZONE` | `string`, in the forms of [§7.6](#76-column-conversion) |
    | `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT` | `int` |
    | `BIT`, `BOOLEAN` | `boolean` |
    | `NUMERIC`, `DECIMAL` | `decimal` |
    | `REAL`, `FLOAT`, `DOUBLE` | `float` |
    | `BINARY`, `VARBINARY`, `LONGVARBINARY`, `BLOB`, `ROWID` | `byte[]` |
    | `ARRAY` | an array, as in [§7.6](#76-column-conversion) rule 3 |
    | `STRUCT`, `REF` | `record {}` |
    | `SQLXML` | `xml` |
    | any other JDBC type | `string` |

### 7.8. Errors

| Condition | Error | When |
|---|---|---|
| A column that matches no field of a closed record type | `sql:FieldMismatchError` | Before the first row |
| A field type that cannot hold the column's JDBC type ([§7.6](#76-column-conversion) rule 2) | `sql:TypeMismatchError` | Before the first row |
| A converted value that does not belong to the field's type, including `NULL` into a field that cannot hold `()` | `sql:TypeMismatchError` | For the row |
| A required field without a default value and without `()` in its type that no column matches | `sql:DataError` (subtype _unspecified_) | For the row |
| A string that is not valid JSON into a `json` target | `sql:ConversionError` | For the row |
| A structured value whose attribute count differs from the field count | `sql:FieldMismatchError` | For the row |
| A column of an unrecognised JDBC type into a record target | `sql:UnsupportedTypeError` | For the row |
| A target type that is not a record type, `T & readonly` or `map<anydata>` ([§7.2](#72-target-record-types)) | `sql:Error` (type _unspecified_) | Before the first row |

Errors are returned, never raised as panics. How an error before the first row and an error for a row are delivered by each operation is specified in [§8](#8-query) and [§9](#9-query-row).

### 7.9. Interactions

- `query`, `queryRow` and the `get` method of an `sql:CursorOutParameter` ([§12](#12-stored-procedure-calls)) map rows by these rules; the `get` method of an OUT parameter converts its value by its own rules ([§12](#12-stored-procedure-calls)).
- A connector MAY override the conversion of any JDBC type ([§17](#17-connector-contract)); the rules of this section are the default behaviour.

### 7.10. Example: Matching with an annotation

```ballerina
type Student record {|
    int id;
    @sql:Column {name: "first_name"}
    string firstName;
    string? email;
|};

function ex7Match(sql:Client db) returns Student[]|sql:Error {
    stream<Student, sql:Error?> students = db->query(`SELECT ID, FIRST_NAME, EMAIL FROM students`);
    return from Student s in students select s;
}
```

For the row `(1, 'Fn', NULL)`, `ID` matches `id` ignoring case, `FIRST_NAME` matches `firstName` through the annotation, and a `NULL` `EMAIL` sets `email` to `()`, giving `{id: 1, firstName: "Fn", email: ()}` ([§7.3](#73-column-matching) rules 2 and 4, [§7.5](#75-building-the-value) rule 1).

### 7.11. Example: Open record with an extra column

```ballerina
type Course record {
    int id;
};

function ex7Open(sql:Client db) returns record {}|sql:Error {
    Course course = check db->queryRow(`SELECT id, title FROM courses WHERE id = 1`);
    return course;
}
```

For the row `(1, 'Maths')` and a driver that reports unquoted identifiers in upper case, the `TITLE` column matches no field and is added as the field `TITLE` of type `string`, giving `{id: 1, TITLE: "Maths"}`. If `Course` were closed, the operation would return an `sql:FieldMismatchError` ([§7.7](#77-unmatched-columns)).

### 7.12. Example: Grouping joined columns

```ballerina
type Teacher record {|
    int id;
    string name;
|};

type Pupil record {|
    int id;
    string name;
    int supervisorId;
    @sql:Column {name: "teachers"}
    Teacher supervisor;
|};

function ex7Group(sql:Client db) returns Pupil|sql:Error {
    return db->queryRow(`SELECT s.id, s.name, s.supervisorId, t.id, t.name
        FROM students s JOIN teachers t ON s.supervisorId = t.id`);
}
```

For a student `(1, 'stu', 10)` whose teacher is `(10, 'tea')`, and a driver that reports unquoted identifiers, including the table name `TEACHERS`, in upper case, the second `ID` and `NAME` columns repeat earlier labels, so their prefix is their table name `TEACHERS`, which equals the group name `teachers` of `supervisor`; they populate `supervisor`, giving `{id: 1, name: "stu", supervisorId: 10, supervisor: {id: 10, name: "tea"}}` ([§7.4](#74-grouping-columns-into-record-fields) rules 2–4).

### 7.13. Example: Conversions

```ballerina
type Event record {|
    string|int id;
    time:Civil startsAt;
    string startsAtText;
    json payload;
|};

function ex7Conv(sql:Client db) returns Event|sql:Error {
    return db->queryRow(`SELECT id, starts_at AS startsAt, starts_at AS startsAtText, payload FROM events`);
}
```

For an `INTEGER` `id` of `7`, a `TIMESTAMP` `starts_at` of `2021-02-03 10:20:30` and a `VARCHAR` `payload` of `{"a":1}`, the result is `{id: 7, startsAt: {year: 2021, month: 2, day: 3, hour: 10, minute: 20, second: 30}, startsAtText: "2021-02-03 10:20:30.0", payload: {a: 1}}`. `id` is an `int` because `int`, the default type of `INTEGER`, is a member of `string|int` ([§7.6](#76-column-conversion) rules 1 and 2).

## 8. Query

### 8.1. Description

The `sql:Client` `query` method executes a query template that produces a result set and returns a stream whose members are the rows of the result set, each converted to the target type.

### 8.2. Behaviour

The target type of `query` is a subtype of `record {}`. The `query` method returns a value of type `stream<T, sql:Error?>`, where `T` is the target type: the stream's completion type is `sql:Error?`, so `next` returns an `sql:Error` for a failure and `()` at the end of the rows. Error streams ([§8.4](#84-error-streams)) and the `queryResult` streams of a `sql:ProcedureCallResult` ([§12.4](#124-procedure-call-result)) have the same completion type. The `query` method never returns an error directly: every failure is delivered through the returned stream.

1. If the client is closed ([§3](#3-client)), `query` returns an error stream ([§8.4](#84-error-streams)) whose error is a `sql:ApplicationError`. No connection is obtained.
2. The client builds the SQL text ([§5.3](#53-sql-text-construction)) and obtains a connection: in a transaction, the connection of the transaction ([§13](#13-transactions)); otherwise a connection from the pool ([§4](#4-connection-pool)).
3. The client prepares the statement, binds the parameters ([§6](#6-parameter-binding)) and executes the statement before `query` returns.
4. The client matches the result set columns against the target type, as specified in [§7](#7-result-mapping). Column-to-field mismatches detected from the result set metadata, such as a column without a field in a closed record, are detected in this step.
5. If any of steps 2 to 4 fails, the client releases the statement and, outside a transaction, the connection, and returns an error stream whose error is the failure ([§8.5](#85-errors)).
6. Otherwise the client returns a stream that holds the result set, the statement and the connection. The stream converts one row each time its `next` method is called ([§8.3](#83-stream-lifecycle)); how many rows the driver fetches from the database ahead of `next` is driver-defined.

### 8.3. Stream lifecycle

The stream returned in step 6 of [§8.2](#82-behaviour) is backed by a `sql:ResultIterator` and is in one of two states, open or closed. It starts open.

1. While open, `next` advances the result set:
    1. If there is a next row, it converts the row to the target type ([§7](#7-result-mapping)) and returns it as `{value: <row>}`.
    2. If there is no next row, it releases the result set, the statement and, outside a transaction, the connection, ignoring any failure to release them, moves the stream to closed and returns `()`.
    3. If reading or converting the row fails, it releases the resources as in rule 1.2, moves the stream to closed, and returns the error ([§8.5](#85-errors)). The rows after the failing row are not returned.
2. While closed, `next` does not access the database. After `next` returned `()`, a later `next` returns `()` again. Otherwise, including after `next` returned an error and after `close`, `next` returns a `sql:ApplicationError`.
3. While open, `close` releases the result set, the statement and, outside a transaction, the connection, and moves the stream to closed. If releasing the result set, the statement or the connection fails, `close` returns a `sql:DatabaseError` and the stream stays open, so `close` can be called again.
4. While closed, `close` does nothing and returns `()`. Calling `close` more than once, or after the stream ended or failed, is therefore not an error.
5. The stream can be iterated only once. Iterating it with a `foreach` statement or a query expression calls `next` until it returns `()` or an error, so iterating to the end, or to the first error, releases the resources without a call to `close`.
6. A stream that is neither iterated to the end nor closed keeps its connection out of the pool until it is closed. While it does, other operations that need a connection from the same pool compete for the remaining connections.
7. When its client is closed, an open stream is closed as by `close` (rule 6 of [§3.3.2](#332-closed-state)), and a later `next` returns a `sql:ApplicationError`.

### 8.4. Error streams

An error stream is a stream that holds one error and no result set. `query` returns one for a failure before or during execution (steps 1 and 5 of [§8.2](#82-behaviour)), and the `sql:generateApplicationErrorStream` function returns one whose error is a `sql:ApplicationError` with the given message.

1. Every call to `next` on an error stream returns its error.
2. `close` on an error stream does nothing and returns `()`. A later call to `next` still returns the error.
3. An error stream holds no connection.

> **Note:** A connector can use `sql:generateApplicationErrorStream` to report a failure from a method that returns a stream.

### 8.5. Errors

| Condition | Error | Delivered |
|---|---|---|
| The client is closed | `sql:ApplicationError` | Error stream |
| No connection is available, or the driver or database fails to prepare or execute the statement, including a statement that does not produce a result set | `sql:DatabaseError` | Error stream |
| A parameter cannot be bound | As specified in [§6](#6-parameter-binding) | Error stream |
| The columns do not match the target type | As specified in [§7](#7-result-mapping), e.g. `sql:FieldMismatchError` | Error stream |
| Any other failure before the stream is returned | `sql:ApplicationError` | Error stream |
| Reading the next row fails in the driver | `sql:DatabaseError` | `next` |
| Converting a row fails | As specified in [§7](#7-result-mapping), e.g. `sql:TypeMismatchError`, `sql:ConversionError` | `next` |
| `next` on a closed stream, except after `next` returned `()` | `sql:ApplicationError` | `next` |
| Releasing resources fails | `sql:DatabaseError` | `close` |

### 8.6. Interactions

- In a transaction, the stream uses the transaction's connection, and neither the end of the stream nor `close` closes that connection. A stream created outside a transaction releases its connection when it ends or is closed, also in a transaction (rule 5 of [§13.2](#132-behaviour)).
- A connector MAY supply its own row iteration through a `sql:CustomResultIterator`; the lifecycle in [§8.3](#83-stream-lifecycle) is that of `sql:ResultIterator` and applies to every connector ([§17](#17-connector-contract)).
- A stream that is not closed counts against `maxOpenConnections` of its pool ([§4](#4-connection-pool)).

### 8.7. Example: Stream lifecycle

The following illustrates rules 1, 2, 3 and 6 of [§8.3](#83-stream-lifecycle).

```ballerina
type Student record {|
    int id;
    string name;
|};

function firstStudent(sql:Client db) returns Student?|sql:Error {
    stream<Student, sql:Error?> students = db->query(`SELECT id, name FROM students ORDER BY id`);
    record {|Student value;|}? first = check students.next();
    check students.close();
    return first?.value;
}
```

The stream holds a connection until `close` is called. After `close`, a further `students.next()` call returns a `sql:ApplicationError`, and a further `students.close()` call returns `()`.

## 9. Query row

### 9.1. Description

The `sql:Client` `queryRow` method executes a query template that produces a result set and returns its first row, or the value of the only column of its first row, converted to the target type.

### 9.2. Behaviour

1. If the client is closed ([§3](#3-client)), `queryRow` returns a `sql:ApplicationError`.
2. The client builds the SQL text ([§5.3](#53-sql-text-construction)), obtains a connection as in step 2 of [§8.2](#82-behaviour), prepares the statement, binds the parameters ([§6](#6-parameter-binding)) and executes it.
3. If the result set has no row, `queryRow` returns a `sql:NoRowsError`, whatever the target type, including a nilable one, so that a missing row can be told apart from a row whose only column is SQL `NULL`.
4. Otherwise it converts the first row to the target type as specified in [§9.3](#93-target-types) and returns the result.
5. Rows after the first are not read and do not cause an error.
6. On success and on every failure after the connection is obtained, the client releases the result set, the statement and, outside a transaction, the connection before `queryRow` returns.

### 9.3. Target types

The target type is a subtype of `anydata`. The first row is converted as follows:

1. **Record target**: when the target type is a record type other than `time:Date`, `time:TimeOfDay` and `time:Civil`, or `T & readonly` for such a record type `T`, or `map<anydata>`, the row is converted as for a stream row ([§7](#7-result-mapping), rule 2 of [§7.2](#72-target-record-types)).
2. **Single-value target**: when the target type is not a record type, or is `time:Date`, `time:TimeOfDay` or `time:Civil`:
    1. If the result set has more than one column, `queryRow` returns a `sql:TypeMismatchError`.
    2. Otherwise the value of the only column is converted to the target type as for a record field of that type ([§7](#7-result-mapping)).
    3. A SQL `NULL` value is returned as `()` when the target type includes `()`. When the target type does not include `()`, `queryRow` returns a `sql:TypeMismatchError`.
3. **Union target**: when the target type is a union type after resolving type references, including an optional type `T?`:
    1. If the union includes `()` and the row has one column whose value is SQL `NULL`, the result is `()`.
    2. Otherwise the client selects a member type other than `()` independently of the order in which the union is written: for a single-column row, the member selected by [§7.6](#76-column-conversion) rule 1; otherwise the single member for which converting the row as in rule 1 (record member) or rule 2 (other member) succeeds.
    3. If several members succeed and none is selected by rule 3.2, `queryRow` returns a `sql:TypeMismatchError`. A database error ends the attempt and is returned.
    4. If no member succeeds, `queryRow` returns a `sql:TypeMismatchError`.
4. A member that is itself a named union type is treated as its own members.
5. An enumeration or other finite type target, or member, is converted as for a record field of that type ([§7](#7-result-mapping)).
6. For an `anydata` target, the value of the only column is converted by rule 2 to the default type of its JDBC type ([§7.7](#77-unmatched-columns)). A target type of `()` alone, or a mapping type other than those of rule 1, is converted by rule 2, and the conversion fails with a `sql:TypeMismatchError` for every column type.
7. For a `json` target, the column value is converted as for a `json` record field ([§7](#7-result-mapping)); for example, a character column is parsed as JSON text.

### 9.4. Errors

| Condition | Error |
|---|---|
| The client is closed | `sql:ApplicationError` |
| No connection is available, or the driver or database fails to prepare or execute the statement, including a statement that does not produce a result set | `sql:DatabaseError` |
| A parameter cannot be bound | As specified in [§6](#6-parameter-binding) |
| The result set has no row | `sql:NoRowsError` |
| A single-value target and more than one column | `sql:TypeMismatchError` |
| SQL `NULL` for a single-value target that does not include `()` | `sql:TypeMismatchError` |
| No member of a union target matches | `sql:TypeMismatchError` |
| The row cannot be converted to a record or single-value target | As specified in [§7](#7-result-mapping) |
| Any other failure | `sql:ApplicationError` |

All errors are returned; `queryRow` does not panic.

### 9.5. Interactions

- In a transaction, `queryRow` uses the transaction's connection and does not close it ([§13](#13-transactions)).
- `queryRow` and the rows of a stream returned by `query` are converted with the connector's result conversion ([§17.5](#175-interactions)).

### 9.6. Example: Single values and no rows

The following illustrates rules 3 and 5 of [§9.2](#92-behaviour) and rule 2 of [§9.3](#93-target-types).

```ballerina
function studentCount(sql:Client db) returns int|sql:Error {
    return db->queryRow(`SELECT COUNT(*) FROM students`);
}

function studentName(sql:Client db, int id) returns string?|sql:Error {
    string|sql:Error name = db->queryRow(`SELECT name FROM students WHERE id = ${id}`);
    if name is sql:NoRowsError {
        return ();
    }
    return name;
}
```

`studentCount` returns the value of the only column. `studentName` returns `()` when no student has the given `id`. `` db->queryRow(`SELECT id, name FROM students`, int) `` returns a `sql:TypeMismatchError` because the result has two columns.

## 10. Execute

### 10.1. Description

The `sql:Client` `execute` method executes a query template that does not produce a result set, such as an `INSERT`, `UPDATE`, `DELETE` or DDL statement, and returns the number of affected rows and a generated key.

### 10.2. Execution result

`execute` returns a `sql:ExecutionResult` record:

| Field | Type | Meaning |
|---|---|---|
| `affectedRowCount` | `int?` | The update count of the statement: the number of rows affected, or `0` for a statement that affects no rows, such as DDL. `sql:SUCCESS_NO_INFO` (`-2`) means the statement succeeded but the count is not known, and `sql:EXECUTION_FAILED` (`-3`) means the statement failed; the driver reports these two only for statements of a batch ([§11](#11-batch-execute)). `execute` never returns `()` in this field. |
| `lastInsertId` | `string\|int?` | The first generated key ([§10.4](#104-generated-keys)), or `()` when there is none. |

Whether generated keys are requested is set by the connector when the client is created, separately for `execute` and for `batchExecute`; the core library has no configuration field for it ([§17.2](#172-connector-obligations), rule 4).

### 10.3. Behaviour

1. If the client is closed ([§3](#3-client)), `execute` returns a `sql:ApplicationError`.
2. The client builds the SQL text ([§5.3](#53-sql-text-construction)) and obtains a connection as in step 2 of [§8.2](#82-behaviour).
3. It prepares the statement, requesting generated keys when the client's connector enables them for `execute` and the SQL text is not a DDL statement (rule 1 of [§10.4](#104-generated-keys)), binds the parameters ([§6](#6-parameter-binding)) and executes the statement as an update.
4. It determines `lastInsertId` as specified in [§10.4](#104-generated-keys).
5. It releases the statement and, outside a transaction, the connection, and returns a `sql:ExecutionResult` with the update count reported by the driver and `lastInsertId`.
6. Outside a transaction, the effect of the statement is committed when it is executed, following the auto-commit setting of the pool's connections; a failure while reading generated keys in step 4 does not undo it.

### 10.4. Generated keys

1. When the SQL text, after leading and trailing white space is removed, starts with `CREATE`, `ALTER`, `DROP`, `TRUNCATE`, `COMMENT` or `RENAME`, compared case-insensitively, the statement is a DDL statement: the client prepares it without a generated-keys request, so the SQL text reaches the database as built ([§5.3](#53-sql-text-construction)), does not read generated keys, and `lastInsertId` is `()`. Any other leading text, including a SQL comment, is not recognised as DDL.
2. Otherwise the client reads the generated keys of the statement. If they contain at least one row, `lastInsertId` is the value of the first column of the first row:
    1. an `int` when the SQL type of that column is `TINYINT`, `SMALLINT`, `INTEGER`, `BIGINT`, `BIT` or `BOOLEAN`;
    2. otherwise its value as a `string`, for example `"1"` for a `DECIMAL` key.
3. When the generated keys have no row, `lastInsertId` is `()`.
4. Which statements produce generated keys, and the order of their rows, is driver-defined. For an `INSERT` of several rows, `lastInsertId` is the key of the first row the driver reports. Whether a driver reports keys for an `UPDATE` is driver-defined.
5. Whether a client receives generated keys when its connector does not request them is driver-defined.
6. Whether generated keys are requested is a property of the client: clients that share a pool ([§4](#4-connection-pool)) each keep their own setting.
7. A connector MAY offer a client option of type `sql:GeneratedKeysMode` ([§17.2](#172-connector-obligations), rule 4), an enumeration with the members `ALL_STATEMENTS`, the default, which requests generated keys as in rules 1–6, and `INSERT_ONLY`. When a client's option is `INSERT_ONLY`, the client requests and reads generated keys only for a statement whose SQL text, after leading white space and SQL comments, starts with `INSERT` or `MERGE`, compared case-insensitively, or with `WITH`, whose statement kind is not determined by its leading word; every other statement is prepared without a generated-keys request, and its `lastInsertId` is `()`.

### 10.5. Errors

| Condition | Error |
|---|---|
| The client is closed | `sql:ApplicationError` |
| No connection is available, or the driver or database fails to prepare or execute the statement or to read its generated keys, including a statement that produces a result set, such as a `SELECT` | `sql:DatabaseError` |
| A parameter cannot be bound | As specified in [§6](#6-parameter-binding) |
| Any other failure | `sql:ApplicationError` |

### 10.6. Interactions

- In a transaction, `execute` uses the transaction's connection, and its effect is committed or rolled back with the transaction ([§13](#13-transactions)).

### 10.7. Example: Insert with a generated key

The following illustrates [§10.3](#103-behaviour) and rule 2 of [§10.4](#104-generated-keys).

```ballerina
function addStudent(sql:Client db, string name) returns int|sql:Error {
    sql:ExecutionResult result = check db->execute(`INSERT INTO students (name) VALUES (${name})`);
    string|int? id = result.lastInsertId;
    return id is int ? id : -1;
}
```

For a table whose `INTEGER` identity column generates the key and a connector that requests generated keys, `result.affectedRowCount` is `1` and `lastInsertId` is the generated `int` key.

## 11. Batch execute

### 11.1. Description

The `sql:Client` `batchExecute` method executes the same SQL text once for each query template of an array, binding the parameters of each template, and returns one result per template.

### 11.2. Behaviour

1. If the client is closed ([§3](#3-client)), `batchExecute` returns a `sql:ApplicationError`.
2. If the array is empty, `batchExecute` returns a `sql:ApplicationError`. No connection is obtained. This check is fixed behaviour ([§17.3](#173-fixed-behaviour), rule 12).
3. The client builds the SQL text of every query template ([§5.3](#53-sql-text-construction)). If the SQL text of any template differs from that of the first, including by white space alone, `batchExecute` returns a `sql:ApplicationError`, and no statement is executed.
4. The client obtains a connection as in step 2 of [§8.2](#82-behaviour) and prepares one statement for the SQL text, requesting generated keys when the client's connector enables them for `batchExecute` and the SQL text is not a DDL statement (rule 1 of [§10.4](#104-generated-keys)).
5. For each query template, in array order, the client binds its parameters ([§6](#6-parameter-binding)) and adds them to the batch. After each 1000th template, it executes the batch of those 1000 templates (a chunk) and appends their results ([§11.3](#113-results)).
6. After the last template, it executes the remaining templates as a final chunk, if any remain.
7. It releases the statement and, outside a transaction, the connection, and returns the results.

### 11.3. Results

1. On success, the result array has one `sql:ExecutionResult` per query template, in the order of the array.
2. `affectedRowCount` is the update count the driver reports for that statement: a count of `0` or more, or `sql:SUCCESS_NO_INFO`.
3. `lastInsertId` is `()` unless the connector enables generated keys for `batchExecute` and the SQL text is not DDL (rule 1 of [§10.4](#104-generated-keys)). Then, within each chunk whose generated keys have exactly one row per statement, the `n`-th row gives `lastInsertId` of the `n`-th statement of the chunk, converted as in rule 2 of [§10.4](#104-generated-keys). When the number of rows differs from the number of statements of the chunk, for example because a statement generates no key or several keys, every statement of the chunk gets `lastInsertId` `()`.

### 11.4. Failure

1. When the driver reports that a statement of a chunk failed, `batchExecute` returns a `sql:BatchExecuteError`. Its `executionResults` detail field holds the results of all chunks executed before the failing chunk, as in [§11.3](#113-results), followed by one entry for each update count that the driver reports for the failing chunk, with `lastInsertId` `()`. Its `errorCode` and `sqlState` fields are those reported by the driver.
2. Whether the driver stops at the failing statement or continues with the remaining statements of the failing chunk, reporting `sql:EXECUTION_FAILED` for each failed statement, is driver-defined, and so is the number of entries rule 1 adds for the failing chunk. Chunks after the failing chunk are not executed.
3. When binding a parameter fails, `batchExecute` returns the binding error ([§6](#6-parameter-binding)), without results. When the driver or database fails otherwise, it returns a `sql:DatabaseError`, without results.
4. A failure does not undo chunks executed before it. Outside a transaction, their effects stay committed according to the auto-commit setting of the pool's connections, and whether the statements of the failing chunk that the driver executed stay committed is driver-defined. In a transaction, they are undone when the transaction rolls back ([§13](#13-transactions)).

### 11.5. Errors

| Condition | Error |
|---|---|
| The client is closed | `sql:ApplicationError` |
| The array is empty | `sql:ApplicationError` |
| The query templates have different SQL text | `sql:ApplicationError` |
| A statement of a chunk fails | `sql:BatchExecuteError` |
| A parameter cannot be bound | As specified in [§6](#6-parameter-binding) |
| No connection is available, or the driver or database fails otherwise, including SQL text that produces a result set | `sql:DatabaseError` |
| Any other failure | `sql:ApplicationError` |

### 11.6. Interactions

- In a transaction, all chunks use the transaction's connection and are committed or rolled back with the transaction ([§13](#13-transactions)).
- The result of a successful `batchExecute` of one query template equals the result of `execute` of that template only when the connector enables generated keys for both methods or for neither ([§10.2](#102-execution-result)).

### 11.7. Example: Batch insert

The following illustrates [§11.3](#113-results) and rule 1 of [§11.4](#114-failure).

```ballerina
function addStudents(sql:Client db, string[] names) returns int|sql:Error {
    sql:ParameterizedQuery[] batch = from string name in names
        select `INSERT INTO students (name) VALUES (${name})`;
    sql:ExecutionResult[]|sql:Error results = db->batchExecute(batch);
    if results is sql:BatchExecuteError {
        return results.detail().executionResults.length();
    }
    return (check results).length();
}
```

All query templates have the SQL text `INSERT INTO students (name) VALUES ( ? )`. On success, the function returns the number of names. If `name` is unique and the second of three names already exists, a driver that stops at the failing statement reports one update count, so the function returns `1`, and outside a transaction the first name stays inserted.

## 12. Stored procedure calls

### 12.1. Description

The `sql:Client` `call` method executes a query template that calls a stored procedure or function. It binds IN parameters, registers OUT and INOUT parameters, executes the statement, and returns a `sql:ProcedureCallResult` that gives access to the results the call produced. OUT and INOUT parameter values are read with the `get` method of the parameter after the call.

### 12.2. Parameter classification

A `sql:ParameterizedCallQuery` is a query template whose parameters are of type `sql:Parameter`: a `sql:Value`, a `sql:InOutParameter`, an object of type `sql:OutParameter`, or a `sql:CursorOutParameter`. The SQL text is constructed from the template as specified in [§5](#5-parameterized-queries), with one placeholder per parameter.

The `call` method classifies each parameter, in template order, as follows:

1. An object whose type name is `InOutParameter` is an INOUT parameter.
2. Otherwise, an object whose type name ends with `OutParameter` is an OUT parameter. This includes every `*OutParameter` class of this module, `sql:CursorOutParameter` (a cursor parameter), and any class with such a name defined by a connector or by the user.
3. Every other parameter, including typed values and non-object values, is an IN parameter and is bound as specified in [§6](#6-parameter-binding).

A `sql:InOutParameter` holds the IN value passed to its constructor, a `sql:Value`. The value cannot be changed after construction.

### 12.3. Call execution

The `call` method takes a query template and `rowTypes`, an array of record `typedesc` values whose default is the empty array.

1. If the client is closed, the method returns a `sql:ApplicationError` and does nothing else.
2. The method obtains a connection: in a transaction, the connection of the transaction ([§13](#13-transactions)); otherwise a connection from the pool ([§4](#4-connection-pool)).
3. It prepares a callable statement from the SQL text.
4. For each parameter, in template order:
    1. An IN parameter is bound as in [§6](#6-parameter-binding).
    2. An INOUT parameter's IN value is bound as in [§6](#6-parameter-binding), and the parameter is registered as an output parameter with the SQL type determined in [§12.5](#125-out-and-inout-parameter-registration).
    3. An OUT parameter is registered as an output parameter with the SQL type determined in [§12.5](#125-out-and-inout-parameter-registration).
5. It executes the statement.
6. If the first result of the execution is a result set, `queryResult` is set to a stream over it, as specified in [§12.4](#124-procedure-call-result), and `executionResult` is `()`. Otherwise, `executionResult` is set to an `sql:ExecutionResult` whose `affectedRowCount` is the update count of the first result and whose `lastInsertId` is `()`, and `queryResult` is `()`.
7. Each OUT and INOUT parameter of the template is bound to this call, so that its `get` method reads the value the call produced ([§12.6](#126-reading-out-and-inout-parameters)).
8. The method returns the `sql:ProcedureCallResult`. The statement and the connection stay open until the `sql:ProcedureCallResult` `close` method is called.

If any step from 2 to 7 fails, the method closes the result set and statement it opened, releases the connection unless it belongs to a transaction, and returns an error as specified in [§12.10](#1210-errors).

Whether the first result of a call is a result set or an update count is driver-defined. For example, a procedure that declares OUT parameters or dynamic result sets may produce an update count before its result sets.

The call syntax inside the template is passed to the JDBC driver unchanged, apart from the placeholders. A procedure is called with `` `{call name(${p1}, ${p2})}` `` or `` `CALL name(${p1})` ``, and a function with `` `{${ret} = call name(${p1})}` ``, where `ret` is an OUT parameter that receives the function's return value. Which of these forms a database supports is driver-defined.

### 12.4. Procedure call result

A `sql:ProcedureCallResult` holds the current result of a call in three public fields:

| Field | Type | Meaning |
|---|---|---|
| `executionResult` | `sql:ExecutionResult?` | The current result when it is an update count; `()` when the current result is a result set. |
| `queryResult` | `stream<record {}, sql:Error?>?` | A stream over the current result when it is a result set. |
| `customResultIterator` | `sql:CustomResultIterator?` | Set by a connector to override `getNextQueryResult` ([§17](#17-connector-contract)). `()` by default. |

#### 12.4.1. Result set streams

1. The result sets of a call are matched to `rowTypes` in the order in which they are produced: the first result set is mapped to `rowTypes[0]`, the second to `rowTypes[1]`, and so on. Update counts do not consume an element of `rowTypes`.
2. When `rowTypes` is empty, each result set is mapped to an open record with an `anydata` rest field, whose field names are the column labels.
3. Rows are mapped to the record type as specified in [§7](#7-result-mapping). A column that cannot be mapped to the record type is reported when the stream is created: by the `call` method for the first result, and by `getNextQueryResult` for later results.
4. When `rowTypes` is not empty, it MUST have one element for each result set the call produces. A `rowTypes` with more elements than the call produces result sets is an error; one with fewer elements is reported as in rule 5.
5. When the call produces more result sets than `rowTypes` has elements, and `rowTypes` is not empty, `getNextQueryResult` returns a `sql:ApplicationError` when it reaches the first result set that has no `typedesc`.
6. Iterating a `queryResult` stream to its end, or closing it, closes its result set only. It does not close the statement or release the connection.
7. A stream that was the `queryResult` before a later `getNextQueryResult` call is closed by that call, as by its `close` method. Iterating it afterwards yields a `sql:ApplicationError` ([§8.3](#83-stream-lifecycle), rule 2).

#### 12.4.2. Moving to the next result

By default, the `getNextQueryResult` method behaves as follows. When `customResultIterator` is not `()`, the method delegates to its `getNextQueryResult` method instead ([§17](#17-connector-contract)).

1. It closes the current result set, if any, and moves to the next result of the statement.
2. If the next result is a result set, it sets `queryResult` to a stream over it (per [§12.4.1](#1241-result-set-streams)), sets `executionResult` to `()`, and returns `true`.
3. Otherwise, it sets `executionResult` to an `sql:ExecutionResult` whose `affectedRowCount` is the update count of the next result and whose `lastInsertId` is `()`, sets `queryResult` to `()`, and returns `false`. When there are no more results, `affectedRowCount` is `-1`.
4. Calling the method again after the results are exhausted returns `false` with `affectedRowCount` `-1`.
5. If moving to the next result or creating its stream fails, the method returns an error. The result that failed is not revisited by later calls, and `executionResult` and `queryResult` keep their previous values. Which element of `rowTypes` later result sets are mapped to after such a failure is _unspecified_.
6. After the `sql:ProcedureCallResult` is closed, the method returns an `sql:Error`.

#### 12.4.3. Closing

1. The `close` method closes the result set of a cursor parameter read from this call, if any, then the statement, which also closes any open result set of the call.
2. It then releases the connection to the pool, unless the call is in a transaction, in which case the connection stays with the transaction.
3. Calling `close` on an already closed `sql:ProcedureCallResult` does nothing and returns `()`. Calling it after the client is closed returns `()`.
4. Until `close` is called, the call holds its connection, even when every result has been consumed. A `sql:ProcedureCallResult` that is never closed keeps its connection out of the pool, so later operations can fail to obtain a connection once the pool is exhausted ([§4](#4-connection-pool)).
5. After `close`, `get` on the call's OUT and INOUT parameters returns an `sql:Error`, and a cursor parameter's `get` returns a stream whose first element is an `sql:Error`.

### 12.5. OUT and INOUT parameter registration

An OUT parameter is registered with the SQL type given by its class:

| OUT parameter class | Registered SQL type |
|---|---|
| `CharOutParameter` | `CHAR` |
| `VarcharOutParameter`, `TextOutParameter` | `VARCHAR` |
| `NCharOutParameter` | `NCHAR` |
| `NVarcharOutParameter` | `NVARCHAR` |
| `ClobOutParameter`, `NClobOutParameter` | `CLOB` |
| `BinaryOutParameter` | `BINARY` |
| `VarBinaryOutParameter` | `VARBINARY` |
| `BlobOutParameter` | `LONGVARBINARY` |
| `SmallIntOutParameter` | `SMALLINT` |
| `IntegerOutParameter` | `INTEGER` |
| `BigIntOutParameter` | `BIGINT` |
| `RealOutParameter` | `REAL` |
| `FloatOutParameter` | `FLOAT` |
| `DoubleOutParameter` | `DOUBLE` |
| `NumericOutParameter` | `NUMERIC` |
| `DecimalOutParameter` | `DECIMAL` |
| `BitOutParameter` | `BIT` |
| `BooleanOutParameter` | `BOOLEAN` |
| `DateOutParameter` | `DATE` |
| `TimeOutParameter` | `TIME` |
| `TimeWithTimezoneOutParameter` | `TIME_WITH_TIMEZONE` |
| `TimestampOutParameter`, `DateTimeOutParameter` | `TIMESTAMP` |
| `TimestampWithTimezoneOutParameter` | `TIMESTAMP_WITH_TIMEZONE` |
| `ArrayOutParameter` and every `*ArrayOutParameter` class | `ARRAY` |
| `RefOutParameter` | `REF` |
| `StructOutParameter` | `STRUCT` |
| `RowOutParameter` | `ROWID` |
| `XMLOutParameter` | `SQLXML` |
| `CursorOutParameter` | `REF_CURSOR` |
| Any other class whose name ends with `OutParameter` | Determined by the connector. By default, the `call` method returns a `sql:DataError`. |

An INOUT parameter is registered with the SQL type that its IN value is bound as:

| IN value | Registered SQL type |
|---|---|
| `string` | `VARCHAR` |
| `int` | `BIGINT` |
| `float` | `DOUBLE` |
| `decimal` | `NUMERIC` |
| `boolean` | `BOOLEAN` |
| `byte[]` | A binary SQL type (_implementation-defined_) |
| Other array values | `ARRAY` |
| `time:Utc`, `time:Civil` | `TIMESTAMP` |
| `time:Date` | `DATE` |
| `time:TimeOfDay` | `TIME` |
| `xml` | `SQLXML` |
| Typed value of this module | The type of the OUT parameter class with the same base name in the table above (for example `sql:NumericValue` registers `NUMERIC`, and `sql:ArrayValue` and every `*ArrayValue` class register `ARRAY`), except that `sql:ClobValue` and `sql:NClobValue` register `LONGVARCHAR`. |
| Other typed value | Determined by the connector. By default, the `call` method returns a `sql:DataError`. |
| `()` | By default, the JDBC type `NULL`, whose value `get` cannot convert, so `get` returns an `sql:Error` for every target type. A connector MAY register it with another type ([§17.4](#174-override-points)). The `call` method logs a warning for each INOUT parameter whose IN value is `()`. To give the SQL type of a `NULL` IN value, use a typed value whose `value` is `()`, such as `new sql:IntegerValue()`. |

### 12.6. Reading OUT and INOUT parameters

The `get` method of an OUT or INOUT parameter takes a target type, which is inferred from the context when omitted.

1. If the parameter has not been passed to a `call` method, `get` returns an `sql:Error`.
2. The value is read from the statement of the most recent call that the parameter was passed to. A parameter passed to several calls reads from the latest one.
3. If reading the value fails, including when that call returned an error, or the `sql:ProcedureCallResult` of that call or the client has been closed, `get` returns an `sql:Error`.
4. If the target type is a union type other than an optional type `T?`, `get` returns a `sql:ApplicationError`. `json` and `anydata` are not union types for this rule. For an optional type `T?`, the value is converted to `T`, and an SQL `NULL` value gives `()`.
5. Otherwise, the value is converted to the target type as specified in [§12.7](#127-out-parameter-type-mapping), and the result is returned.

`get` can be called any number of times; each call reads the value again and returns the same value. Reading a value does not change the parameter or the `sql:ProcedureCallResult`.

Whether reading an OUT or INOUT parameter before the result sets of the call are consumed affects those result sets is driver-defined. For example, a driver may close the current result set when an output parameter is read.

### 12.7. OUT parameter type mapping

By default, the value of an OUT parameter is converted to the target type according to the registered SQL type ([§12.5](#125-out-and-inout-parameter-registration)) as follows. A target type not listed for the registered SQL type results in a `sql:TypeMismatchError`. An INOUT parameter is converted in the same way, using its registered SQL type; an INOUT parameter registered as `ARRAY` is converted as an `sql:ArrayOutParameter`. A connector MAY extend this mapping for types it registers ([§17](#17-connector-contract)), and the connector's conversion applies to the OUT and INOUT parameter classes of this library as well as to its own ([§17.5](#175-interactions)).

| Registered SQL type | Target type | Result |
|---|---|---|
| `CHAR`, `VARCHAR`, `LONGVARCHAR`, `NCHAR`, `NVARCHAR`, `CLOB` | `string`, `json` | The character value as a `string`. For a `json` target the result is that `string`; the text is not parsed as JSON. |
| `BINARY`, `VARBINARY`, `LONGVARBINARY` | `byte[]` | The bytes of the value. |
| | `string` | The driver's string representation of the value (driver-defined). |
| `SMALLINT`, `INTEGER`, `BIGINT` | `int` | The value. |
| | `string` | The decimal representation of the value. |
| `REAL`, `FLOAT`, `DOUBLE` | `float` | The value. A `REAL` value is widened from single precision. |
| | `string` | The string representation of the `float` value. |
| `NUMERIC`, `DECIMAL` | `decimal` | The value. |
| | `string` | The string representation of the value. |
| | `int` | _Unspecified_. |
| `BIT`, `BOOLEAN` | `boolean` | The value. |
| | `int` | `1` for true, `0` for false. |
| | `string` | `"true"` or `"false"`. |
| `DATE` | `time:Date` | The date. |
| | `int` | Milliseconds since the epoch of the start of the date in the time zone of rule 5.1 of [§7.6](#76-column-conversion). |
| | `string` | The date in `yyyy-mm-dd` format. |
| `TIME` | `time:TimeOfDay` | The hour, minute and whole second; fractional seconds are discarded. |
| | `int` | Milliseconds since the epoch of the time on 1970-01-01 in the time zone of rule 5.1 of [§7.6](#76-column-conversion). |
| | `string` | The time in `hh:mm:ss` format. |
| `TIME_WITH_TIMEZONE` | `time:TimeOfDay` | The time, with `utcOffset` set to the value's offset. |
| | `int` | Milliseconds since the epoch of the instant of the time on 1970-01-01 at the value's offset. |
| | `string` | ISO 8601 local time with offset: `hh:mm`, then `:ss` when the seconds or the fraction are not zero, then the fraction in 3, 6 or 9 digits when it is not zero, then the offset as `±hh:mm`, or `Z` for UTC; for example `10:20:30.500+05:30` or `10:20Z`. |
| `TIMESTAMP` | `time:Civil` | The date and time, without `utcOffset`. |
| | `time:Utc` | The instant of the value interpreted in the time zone of rule 5.1 of [§7.6](#76-column-conversion). |
| | `int` | Milliseconds since the epoch of the value interpreted in the time zone of rule 5.1 of [§7.6](#76-column-conversion). |
| | `string` | `yyyy-mm-dd hh:mm:ss.f…`, with one to nine fractional digits, trailing zeros removed, and at least one fractional digit. |
| `TIMESTAMP_WITH_TIMEZONE` | `time:Civil` | The date and time, with `utcOffset` set to the value's offset. |
| | `time:Utc` | The instant of the value. |
| | `int` | Milliseconds since the epoch of the instant of the value. |
| | `string` | `yyyy-mm-dd`, then `T`, then the time and offset in the form of the `TIME_WITH_TIMEZONE` row; for example `2021-02-03T10:20:30.250+05:30` or `2021-02-03T10:20Z`. |
| `REF`, `STRUCT` | A record type | A record whose fields are the attributes of the structured value in order. A different number of attributes and fields results in a `sql:FieldMismatchError`. |
| `ROWID` | `byte[]` | The bytes of the row identifier. |
| `SQLXML` | `xml` | The parsed XML value. |
| `ARRAY` | An array type | As specified in the array table below. |

When the value of an OUT or INOUT parameter is SQL `NULL`, `get` returns `()` for an optional target type `T?` (rule 4 of [§12.6](#126-reading-out-and-inout-parameters)), and an `sql:Error` for any other target type.

For an array OUT parameter, the target type is matched by its name, so `int[]` matches but `int?[]` and `(int[] & readonly)` do not. The elements are converted as follows:

| OUT parameter class | Target types |
|---|---|
| `CharArrayOutParameter`, `VarcharArrayOutParameter`, `NVarcharArrayOutParameter` | `string[]` |
| `BitArrayOutParameter` | `boolean[]`, `int[]` (`1` or `0`), `string[]` |
| `BooleanArrayOutParameter` | `boolean[]`, `int[]` (`1` or `0`), `string[]` |
| `SmallIntArrayOutParameter`, `IntegerArrayOutParameter`, `BigIntArrayOutParameter` | `int[]`, `string[]` |
| `RealArrayOutParameter`, `DoubleArrayOutParameter` | `float[]`, `decimal[]`, `int[]` (truncated towards zero), `string[]` |
| `FloatArrayOutParameter` | `float[]`, `int[]` (truncated towards zero), `string[]` |
| `NumericArrayOutParameter`, `DecimalArrayOutParameter` | `decimal[]`, `float[]`, `int[]` (truncated towards zero), `string[]` |
| `DateArrayOutParameter` | `time:Date[]`, `string[]` |
| `TimeArrayOutParameter`, `TimeWithTimezoneArrayOutParameter` | `time:TimeOfDay[]`, `string[]` |
| `DateTimeArrayOutParameter`, `TimestampArrayOutParameter`, `TimestampWithTimezoneArrayOutParameter` | `time:Civil[]`, `string[]` |
| `BinaryArrayOutParameter`, `VarBinaryArrayOutParameter` | `byte[][]`, `string[]` |
| `ArrayOutParameter` | `string[]`, `boolean[]`, `int[]`, `float[]`, `decimal[]`, `byte[][]`, `time:Date[]`, `time:TimeOfDay[]`, `time:Civil[]`; the elements are converted from the element values the driver returns. By default, any other array type results in a `sql:UnsupportedTypeError`. |

When the SQL array is empty, `get` returns an empty array of the target type. An SQL `NULL` array is handled as any SQL `NULL` value (see above). When the array contains SQL `NULL` elements, the result of `get` is _unspecified_.

### 12.8. Cursor parameters

A `sql:CursorOutParameter` is registered as `REF_CURSOR`. Its `get` method takes a record `typedesc`, inferred from the context when omitted, and returns a stream of that record type.

1. If the parameter has not been passed to a `call` method, the stream's first element is an `sql:Error`.
2. The cursor is read from the statement of the most recent call the parameter was passed to. If reading it fails, including after the `sql:ProcedureCallResult` is closed, the stream's first element is an `sql:Error`.
3. If the cursor is SQL `NULL`, the result is _unspecified_.
4. Otherwise, the rows of the cursor are mapped to the record type as in [§7](#7-result-mapping), and the stream yields them. A column that cannot be mapped to the record type is reported as the stream's first element, the error given in [§7.8](#78-errors).
5. Whether `get` can be called more than once for the same call is driver-defined. When the cursor cannot be read again, the stream's first element is an `sql:Error`.
6. The result set of the cursor is closed when its stream is iterated to the end or closed, or when the `sql:ProcedureCallResult` is closed.

Whether a database returns a cursor outside a transaction is driver-defined. For example, PostgreSQL returns a cursor only in a transaction.

### 12.9. Compile-time validation

The compiler plugin checks calls to the `get` method of the OUT parameter classes of this module whose argument is a type name. It reports an error when the target type is not accepted for the class:

| Code | OUT parameter classes | Accepted target types |
|---|---|---|
| `SQL_201` | `ArrayOutParameter` and every `*ArrayOutParameter` class | An array type |
| `SQL_202` | `RefOutParameter`, `StructOutParameter` | A record type |
| `SQL_203` | `XMLOutParameter` | `xml` |
| `SQL_204` | `RowOutParameter` | An array type |
| `SQL_211` | `CharOutParameter`, `VarcharOutParameter`, `TextOutParameter`, `NCharOutParameter`, `NVarcharOutParameter`, `ClobOutParameter`, `NClobOutParameter` | `string`, `json` |
| `SQL_212` | `BinaryOutParameter`, `VarBinaryOutParameter`, `BlobOutParameter` | An array type, `string` |
| `SQL_213` | `SmallIntOutParameter`, `IntegerOutParameter`, `BigIntOutParameter` | `int`, `string` |
| `SQL_214` | `RealOutParameter`, `FloatOutParameter`, `DoubleOutParameter` | `float`, `string` |
| `SQL_215` | `NumericOutParameter`, `DecimalOutParameter` | `decimal`, `string` |
| `SQL_221` | `BitOutParameter`, `BooleanOutParameter` | `boolean`, `int`, `string` |
| `SQL_222` | `DateOutParameter` | `time:Date`, `int`, `string` |
| `SQL_223` | `TimeOutParameter`, `TimeWithTimezoneOutParameter` | `time:TimeOfDay`, `int`, `string` |
| `SQL_231` | `TimestampOutParameter`, `DateTimeOutParameter`, `TimestampWithTimezoneOutParameter` | `time:Civil`, `time:Utc`, `int`, `string` |

1. A type name that refers to a union type is not accepted by any class, except an optional type `T?` whose `T` the class accepts.
2. The check applies only when the static type of the expression on which `get` is called is one of the classes above, `get` has exactly one argument, and the package has no other compilation errors.
3. The following are not checked at compile time, and are validated only when `get` runs ([§12.6](#126-reading-out-and-inout-parameters)): a target type inferred from the context, a target type passed as a `typedesc` value other than a type name, `sql:InOutParameter`, `sql:CursorOutParameter`, and a variable whose static type is `sql:OutParameter`.

All diagnostics have severity error.

### 12.10. Errors

| Condition | Result |
|---|---|
| `call` on a closed client | `sql:ApplicationError`, returned |
| The database or driver reports a failure while preparing, binding, registering or executing the call | `sql:DatabaseError`, returned by `call` |
| An IN or INOUT value cannot be bound ([§6](#6-parameter-binding)), or an OUT parameter class or typed value has no registration type | The `sql:ApplicationError` subtype of the failure (for example `sql:DataError`), returned by `call` |
| The first result set cannot be mapped to `rowTypes[0]` | The error given in [§7.8](#78-errors), such as `sql:FieldMismatchError` or `sql:TypeMismatchError`, returned by `call` |
| `getNextQueryResult`: the database reports a failure while moving to the next result | `sql:DatabaseError`, returned |
| `getNextQueryResult`: a result set cannot be mapped to its `typedesc` | The error given in [§7.8](#78-errors), returned |
| `getNextQueryResult`: a result set has no `typedesc` | `sql:ApplicationError`, returned |
| `getNextQueryResult` after `close` | `sql:Error`, returned |
| `close`: the database reports a failure while closing | `sql:DatabaseError`, returned |
| `get` before the parameter is passed to `call` | `sql:Error`, returned |
| `get`: reading the value from the statement fails (including after `close`) | `sql:Error`, returned |
| `get` with a union target type other than `T?` | `sql:ApplicationError`, returned |
| `get` of an SQL `NULL` value with a target type other than `T?` | `sql:Error`, returned |
| `get` with a target type not accepted for the registered SQL type | `sql:TypeMismatchError`, returned |
| `get` on a value whose registered SQL type has no conversion in [§12.7](#127-out-parameter-type-mapping) or in the connector's mapping (for example an INOUT parameter registered with a connector-specific type) | `sql:Error` by default, returned |
| `get` on an `ArrayOutParameter` or `ARRAY` INOUT parameter with an unsupported array target type | `sql:UnsupportedTypeError` by default, returned |
| `get`: any other conversion failure | `sql:Error` or a subtype, returned |
| Cursor `get` failures | The first element of the returned stream |

Error messages are not part of the contract, except that a `call` failure reported by the database starts with `Error while executing SQL query: ` followed by the SQL text.

### 12.11. Interactions

- In a transaction, the call uses the transaction's connection, and `close` leaves it open ([§13](#13-transactions)). A `sql:ProcedureCallResult` created outside a transaction releases its connection when it is closed, also in a transaction (rule 5 of [§13.2](#132-behaviour)).
- A `sql:ProcedureCallResult` that is not closed holds a pool connection ([§4](#4-connection-pool)).
- Closing the client ([§3](#3-client)) closes the pool. Afterwards, `close` on an open `sql:ProcedureCallResult` returns `()`, and reading parameters or results of the call returns errors.
- IN parameters and INOUT IN values are bound as specified in [§6](#6-parameter-binding), or as the connector overrides it ([§17.4](#174-override-points)), as for `execute`; OUT parameter registration and value conversion are connector override points ([§17](#17-connector-contract)).
- `queryResult` streams follow the stream rules of [§8](#8-query), except that reaching their end or closing them does not release the connection.

### 12.12. Example: OUT and INOUT parameters

The procedure `count_orders` has an IN, an OUT and an INOUT parameter.

```ballerina
function countOrders(sql:Client db) returns error? {
    sql:IntegerOutParameter total = new;
    sql:InOutParameter counter = new (41);
    sql:ProcedureCallResult result = check db->call(`CALL count_orders(${"US"}, ${total}, ${counter})`);
    int t = check total.get(int);
    string c = check counter.get(string);
    check result.close();
}
```

`"US"` is bound as an IN parameter ([§12.2](#122-parameter-classification)). `counter` is registered as `BIGINT` because its IN value is an `int` ([§12.5](#125-out-and-inout-parameter-registration)), so `get(string)` returns its value as a decimal string, for example `"42"` when the procedure increments it ([§12.7](#127-out-parameter-type-mapping)). `total.get(float)` would be rejected at compile time with `SQL_213` ([§12.9](#129-compile-time-validation)).

### 12.13. Example: Multiple result sets

The procedure `customer_orders` returns two result sets, customers and then orders.

```ballerina
type Customer record {| int id; string name; |};
type Order record {| int id; decimal amount; |};

function customerOrders(sql:Client db) returns error? {
    sql:ProcedureCallResult result = check db->call(`CALL customer_orders(${1})`, [Customer, Order]);
    boolean isQueryResult = result.queryResult !is ();
    while isQueryResult || result.executionResult?.affectedRowCount != -1 {
        stream<record {}, sql:Error?>? rows = result.queryResult;
        if isQueryResult && rows is stream<record {}, sql:Error?> {
            check from record {} row in rows
                do {
                    // The first result set has Customer rows, the second has Order rows.
                };
        }
        isQueryResult = check result.getNextQueryResult();
    }
    check result.close();
}
```

The first result set is mapped to `Customer` and the second to `Order`, whether or not the database reports an update count before them ([§12.4.1](#1241-result-set-streams)). The loop ends when `getNextQueryResult` returns `false` with `affectedRowCount` `-1` ([§12.4.2](#1242-moving-to-the-next-result)). With only `[Customer]` as `rowTypes`, the `getNextQueryResult` call that reaches the orders result set returns a `sql:ApplicationError` (rule 5 of [§12.4.1](#1241-result-set-streams)).

### 12.14. Example: Function call

```ballerina
function greet(sql:Client db) returns error? {
    sql:VarcharOutParameter greeting = new;
    sql:ProcedureCallResult result = check db->call(`{${greeting} = call make_greeting(${"Ann"})}`);
    string g = check greeting.get(string);
    check result.close();
}
```

`greeting` is an OUT parameter that receives the return value of the function `make_greeting`, if the JDBC driver supports this syntax ([§12.3](#123-call-execution)).

### 12.15. Example: Cursor parameter

```ballerina
type Item record {| int id; string name; |};

function openItems(sql:Client db) returns error? {
    sql:CursorOutParameter items = new;
    sql:ProcedureCallResult result = check db->call(`{${items} = call open_items()}`);
    stream<Item, sql:Error?> rows = items.get();
    Item[] list = check from Item i in rows select i;
    check result.close();
}
```

`items.get()` infers `Item` from the context and returns the cursor's rows as a stream ([§12.8](#128-cursor-parameters)). Had `result.close()` been called before `items.get()`, the stream's first element would be an `sql:Error` ([§12.4.3](#1243-closing)).

## 13. Transactions

### 13.1. Description

Operations executed inside a Ballerina `transaction` statement take part in the transaction: their changes are committed or rolled back together when the transaction ends.

### 13.2. Behaviour

1. An operation is in a transaction when it is executed while a transaction is active and the current transaction context has a transaction block.
2. Outside a transaction, every operation uses a connection in auto-commit mode, so each statement is committed when it completes.
3. The first operation of a client in a transaction obtains a connection from the client's pool, turns auto-commit off, and registers the connection as a participant of the transaction. Every later operation of the same client in the same transaction uses that connection.
4. Each client is a separate participant, even when clients share a pool: two clients in one transaction use two connections.
5. Releasing the resources of an operation that used the connection of a transaction (closing a stream or a `sql:ProcedureCallResult`, or completing `execute`, `batchExecute`, `queryRow` or `call`) does not return that connection to the pool. A stream or a `sql:ProcedureCallResult` created outside a transaction returns its connection to the pool when it is closed or consumed, also when that happens in a transaction.
6. When the transaction commits, each participant's connection is committed and returned to the pool. When the transaction rolls back, for a `rollback` statement, a failure of the transaction block, or a panic, each participant's connection is rolled back and returned to the pool.
7. Participants that are not XA resources are committed one after another, each on its own. The library gives no atomicity across them: if one commit fails, the others are not undone.
8. An `sql:Error` returned by an operation does not end the transaction. The transaction rolls back only if the transaction block fails or executes `rollback`; otherwise the changes of the other operations are committed.
9. `sql:Error` values are not `error:Retriable`. With the default retry manager, a `retry transaction` statement whose block fails with an `sql:Error` is not retried.
10. For an XA data source ([§4.8](#48-xa-data-sources)), the connection is enlisted as an XA resource and committed with two-phase commit by the Ballerina transaction coordinator: through the Ballerina transaction manager when it is enabled, and through the connection's XA resource otherwise.
11. The isolation level of the transaction is that of the pool's connections ([§4.4](#44-transaction-isolation)). It is set per pool, not per transaction; to use another level, use a client with a pool configuration that sets it.
12. A `transaction` statement executed while a transaction is already active begins an independent transaction, as the Ballerina language specification defines: an operation of a client in the inner block uses a connection of its own, not the connection the client uses in the outer transaction, and the inner transaction commits or rolls back independently of the outer one. When the inner block needs a lock that the outer transaction holds, it waits until the database's lock timeout, if any, and otherwise forever, because the outer transaction cannot proceed until the inner block ends. Nested `transaction` statements are not mapped to savepoints; a partial rollback inside a transaction uses the database's `SAVEPOINT` statements through `execute`.

> **Note:** The Ballerina language does not allow a strand to be started in a transactional scope, so the operations of a transaction are executed one at a time.

### 13.3. Errors

| Condition | Error |
|---|---|
| The participant's connection cannot be obtained | `sql:DatabaseError` returned by the operation |
| The database rejects a statement | the operation's error ([§15](#15-errors)); the transaction stays active |
| Commit of a participant fails | the `commit` action returns an error; its type and message are _unspecified_ |

### 13.4. Interactions

- A client in a transaction holds a connection of its pool until the transaction ends ([§4.11](#411-interactions)); when the pool has no free connection, the operation waits up to `connectionTimeout` ([§3.4](#34-concurrency)).
- Closing a client that participates in a transaction: see rule 7 of [§3.3.2](#332-closed-state).

### 13.5. Example: Local transaction

```ballerina
import ballerina/sql;

function transfer(sql:Client db, int fromId, int toId, decimal amount) returns error? {
    transaction {
        _ = check db->execute(`UPDATE accounts SET balance = balance - ${amount} WHERE id = ${fromId}`);
        _ = check db->execute(`UPDATE accounts SET balance = balance + ${amount} WHERE id = ${toId}`);
        check commit;
    }
}
```

Both statements run on one connection with auto-commit off (rule 3 of [§13.2](#132-behaviour)). If the second `execute` returns an error, `check` fails the transaction block, and the first update is rolled back (rule 6).

## 14. Metadata retrieval

### 14.1. Description

`sql:SchemaClient` is the object type of a client that retrieves the tables and routines of one database. The `sql` library defines the type and the records it returns; a connector provides the class and its initialization, including which database it describes.

### 14.2. Operations

`sql:SchemaClient` is a client object type with four remote methods and a `close` method. Each method returns an `sql:Error` when retrieval fails.

1. `listTables` returns the names of the tables and views of the database, or an empty array if there are none.
2. `getTableInfo` returns an `sql:TableDefinition` for the table or view with the given name. If the database has no such table, or the user cannot see it, it returns an `sql:NoRowsError`. The `include` argument, `COLUMNS_ONLY` by default, selects the optional fields of the result:

    | `include` | `columns` | `referentialConstraints` of each column | `checkConstraints` |
    |---|---|---|---|
    | `NO_COLUMNS` | absent | – | absent |
    | `COLUMNS_ONLY` | present | absent | absent |
    | `COLUMNS_WITH_CONSTRAINTS` | present | present for a column that has foreign keys, absent otherwise | present if the table has check constraints, absent otherwise |

3. `listRoutines` returns the names of the procedures and functions of the database, or an empty array if there are none.
4. `getRoutineInfo` returns an `sql:RoutineDefinition` for the routine with the given name. If the database has no such routine, or the user cannot see it, it returns an `sql:NoRowsError`.
5. `close` releases the client's resources. The result of calling a method, or `close` again, after `close` is _implementation-defined_ by the connector.

### 14.3. Metadata types

The records are open, so a connector can add database-specific fields.

| Record | Field | Type | Meaning |
|---|---|---|---|
| `TableDefinition` | `name` | `string` | Name of the table |
| | `type` | `sql:TableType` | Base table or view |
| | `columns` | `sql:ColumnDefinition[]`, optional | Columns of the table |
| | `checkConstraints` | `sql:CheckConstraint[]`, optional | Check constraints of the table |
| `ColumnDefinition` | `name` | `string` | Name of the column |
| | `type` | `string` | SQL data type name, as reported by the database |
| | `defaultValue` | `anydata?` | Default value of the column as reported by the database, `()` when there is none; its form is _implementation-defined_ by the connector |
| | `nullable` | `boolean` | Whether the column accepts `NULL` |
| | `referentialConstraints` | `sql:ReferentialConstraint[]`, optional | Foreign keys of the column |
| `ReferentialConstraint` | `name` | `string` | Name of the constraint |
| | `tableName` | `string` | Table of the referenced column |
| | `columnName` | `string` | Referenced column |
| | `updateRule` | `sql:ReferentialRule` | Action on update of the referenced key |
| | `deleteRule` | `sql:ReferentialRule` | Action on delete of the referenced key |
| `CheckConstraint` | `name` | `string` | Name of the constraint |
| | `clause` | `string` | Text of the check condition |
| `RoutineDefinition` | `name` | `string` | Name of the routine |
| | `type` | `sql:RoutineType` | Procedure or function |
| | `returnType` | `string?` | SQL data type of the return value, `()` for a routine that returns none |
| | `parameters` | `sql:ParameterDefinition[]` | Parameters, empty when there are none |
| `ParameterDefinition` | `mode` | `sql:ParameterMode` | Parameter mode |
| | `name` | `string` | Parameter name |
| | `type` | `string` | SQL data type name |

| Enumeration | Members (string value) |
|---|---|
| `TableType` | `BASE_TABLE` (`"BASE TABLE"`), `VIEW` (`"VIEW"`) |
| `ReferentialRule` | `NO_ACTION` (`"NO ACTION"`), `RESTRICT`, `CASCADE`, `SET_NULL` (`"SET NULL"`), `SET_DEFAULT` (`"SET DEFAULT"`) |
| `RoutineType` | `PROCEDURE`, `FUNCTION` |
| `ParameterMode` | `IN`, `OUT`, `INOUT` |
| `ColumnRetrievalOptions` | `NO_COLUMNS`, `COLUMNS_ONLY`, `COLUMNS_WITH_CONSTRAINTS` |

The string value of a member without a value in parentheses is its name.

### 14.4. Errors

| Condition | Error |
|---|---|
| Retrieval fails | `sql:Error`, returned; the subtype is _implementation-defined_ by the connector |
| `getTableInfo`: the database has no table or view with the given name, or the user cannot see it | `sql:NoRowsError`, returned |
| `getRoutineInfo`: the database has no routine with the given name, or the user cannot see it | `sql:NoRowsError`, returned |

### 14.5. Example: Table information

```ballerina
import ballerina/sql;

function foreignKeys(sql:SchemaClient schema, string tableName) returns string[]|sql:Error {
    sql:TableDefinition 'table = check schema->getTableInfo(tableName, sql:COLUMNS_WITH_CONSTRAINTS);
    string[] keys = [];
    foreach sql:ColumnDefinition column in 'table.columns ?: [] {
        foreach sql:ReferentialConstraint fk in column.referentialConstraints ?: [] {
            keys.push(column.name + " -> " + fk.tableName + "." + fk.columnName);
        }
    }
    return keys;
}
```

With `COLUMNS_WITH_CONSTRAINTS`, `columns` is present and each column that has a foreign key carries `referentialConstraints` (rule 2 of [§14.2](#142-operations)). For a table that does not exist, the function returns the `sql:NoRowsError` of `getTableInfo`.

## 15. Errors

### 15.1. Description

Every failure of the library is reported as a value of a subtype of `sql:Error`, returned to the caller. The type identifies the kind of failure.

### 15.2. Error types

All types are distinct error types.

| Type | Parent | Detail | Meaning |
|---|---|---|---|
| `Error` | – | – | Any failure of the library |
| `DatabaseError` | `Error` | `DatabaseErrorDetail` | A database error |
| `BatchExecuteError` | `Error` | `BatchExecuteErrorDetail` | A database error while executing a batch ([§11](#11-batch-execute)) |
| `NoRowsError` | `Error` | – | A query that must return a row returned none |
| `ApplicationError` | `Error` | – | A failure in the library or its configuration, not reported by the database |
| `DataError` | `ApplicationError` | – | A failure while processing parameters or results |
| `TypeMismatchError` | `DataError` | – | A value is incompatible with the SQL or Ballerina type it must be bound as or mapped to |
| `ConversionError` | `DataError` | – | A parameter or result value cannot be converted to the required form, such as an invalid date or invalid JSON text |
| `FieldMismatchError` | `DataError` | – | A result cannot be mapped to the fields of the target record type |
| `UnsupportedTypeError` | `DataError` | – | A parameter or result has a type that the library does not support |

`sql:BatchExecuteError` is not a subtype of `sql:DatabaseError`.

| Detail record | Field | Type | Meaning |
|---|---|---|---|
| `DatabaseErrorDetail` | `errorCode` | `int` | Vendor error code reported by the driver |
| | `sqlState` | `string?` | SQLSTATE reported by the driver, `()` when the driver reports none |
| `BatchExecuteErrorDetail` | `errorCode` | `int` | As above |
| | `sqlState` | `string?` | As above |
| | `executionResults` | `sql:ExecutionResult[]` | One element per update count the driver reported for the batch, in order; see [§11](#11-batch-execute) |

### 15.3. Mapping

1. A database error reported during batch execution as a batch update failure produces an `sql:BatchExecuteError`.
2. Any other database error produces an `sql:DatabaseError`, with `errorCode` and `sqlState` taken from the driver's exception. This includes constraint violations, syntax errors, missing tables and failures to obtain a connection.
3. A `queryRow` call whose query returns no row produces an `sql:NoRowsError` ([§9](#9-query-row)).
4. Failures detected by the library while processing parameters and results produce the `sql:DataError` subtype stated in the section that specifies them ([§6](#6-parameter-binding), [§7](#7-result-mapping), [§12](#12-stored-procedure-calls)); other failures detected by the library, such as configuration errors and use of a closed client, produce an `sql:ApplicationError`.
5. Any other unexpected failure produces an `sql:ApplicationError`, in every operation.
6. The `get` method of an OUT or INOUT parameter reports a failure to read the parameter value from the statement as an `sql:DatabaseError` when the driver reports a database error, and as an `sql:ApplicationError` otherwise.
7. Errors are returned to the caller. Errors have no cause; the vendor error code and the SQLSTATE of a database error are in its detail record ([§15.2](#152-error-types)).
8. A message that includes the SQL text includes it with each parameter replaced by `?`; parameter values are not included. Java class name prefixes (`a.b.C: `) are removed from messages. The rest of the message text is _unspecified_.

### 15.4. Example: Database error detail

```ballerina
import ballerina/sql;

function insertOnce(sql:Client db, int id) returns boolean|sql:Error {
    sql:ExecutionResult|sql:Error result = db->execute(`INSERT INTO items (id) VALUES (${id})`);
    if result is sql:DatabaseError {
        return result.detail().sqlState == "23505";
    }
    if result is sql:Error {
        return result;
    }
    return false;
}
```

When the row already exists, the database reports a unique constraint violation; `execute` returns an `sql:DatabaseError` whose `sqlState` is the SQLSTATE the driver reports, `23505` for databases that follow the SQL standard (rule 2 of [§15.3](#153-mapping)).

## 16. Observability

### 16.1. Description

When Ballerina metrics are enabled, every pool reports metrics on its connections through the Ballerina observability module.

### 16.2. Behaviour

1. Metrics are registered for a pool when, at the time the pool is created, Ballerina metrics are enabled, which requires the program to be built with observability included and `metricsEnabled` set to `true` in the `[ballerina.observe]` configuration. Otherwise no metrics are registered.
2. XA pools ([§4.8](#48-xa-data-sources)) have no metrics.
3. The pool gauges and the initialization time gauge are registered when the pool is created. A connection event metric is registered when its first event occurs.
4. All metrics of a pool are unregistered when the pool is shut down ([§3.3.2](#332-closed-state)), and when pool creation fails.
5. A failure to register or update a metric does not affect the pool or any operation.

### 16.3. Metrics

| Name | Kind | Value |
|---|---|---|
| `sql_pool_active_connections` | gauge, read on collection | Connections in use |
| `sql_pool_idle_connections` | gauge, read on collection | Idle connections |
| `sql_pool_total_connections` | gauge, read on collection | Connections in use and idle |
| `sql_pool_pending_requests` | gauge, read on collection | Strands waiting for a connection |
| `sql_pool_max_connections` | gauge, read on collection | Effective `maxOpenConnections` |
| `sql_pool_min_connections` | gauge, read on collection | Effective `minIdleConnections` ([§4.7](#47-pool-creation), rule 6) |
| `sql_pool_utilization_ratio` | gauge, read on collection | Connections in use divided by `sql_pool_max_connections`, `0` when the maximum is `0` |
| `sql_pool_initialization_time_seconds` | gauge, set once | Seconds taken to create the pool, including its first connection attempt |
| `sql_connection_acquisition_time_seconds` | gauge with summary | Seconds an operation waited for a connection, recorded for each acquisition, including timed-out ones |
| `sql_connection_usage_time_seconds` | gauge with summary | Seconds a connection was out of the pool, in millisecond resolution |
| `sql_connection_creation_time_seconds` | gauge with summary | Seconds taken to open a new connection, in millisecond resolution |
| `sql_connection_timeout_total` | counter | Acquisitions that failed because `connectionTimeout` elapsed |

The summaries have the percentiles 0.5, 0.75, 0.95 and 0.99 over a 10-minute window.

> **Note:** The exposition format is defined by the metrics reporter. For example, the Prometheus reporter exposes a gauge `sql_pool_active_connections` as `sql_pool_active_connections_value`, and replaces each `-` in a tag value with `_`.

### 16.4. Tags

1. Every metric of a pool has the tag `pool_name`.
2. When `poolName` is given, the tag value is `poolName` with leading and trailing whitespace removed, each character other than an ASCII letter, a digit, `.`, `_` or `-` replaced by `-`, each run of `-` replaced by a single `-`, and a leading or trailing `-` removed. When `poolName` is `()`, or the result of this transformation is empty, the tag value is the name the pool generates.
3. A connector can add the tags `db_host`, `db_port` and `db_name` ([§17](#17-connector-contract)). The library adds no other tag; in particular, no tag contains the URL, the user or the password.
4. The `pool_name` values of active pools are distinct. When the value of a new pool equals that of an active pool, a sequence number is appended to it, for example `orders-2`; the exact form is _implementation-defined_. Each pool therefore reports its own metrics, and shutting down a pool removes only its own metrics.

### 16.5. Example: Enabling metrics

```toml
[ballerina.observe]
metricsEnabled = true
metricsReporter = "prometheus"
```

With this configuration and a program built with observability included, a client created with `connectionPool = {poolName: "Orders DB"}` reports `sql_pool_active_connections` and the other metrics with the tag `pool_name` set to `Orders-DB` (rule 2 of [§16.4](#164-tags)) until its pool is shut down (rule 4 of [§16.2](#162-behaviour)).

## 17. Connector contract

### 17.1. Description

A connector provides the client class, its initialization and its options, and delegates the operations to the library. This section states what a connector MUST do, what the library guarantees for every connector, and which default behaviour a connector MAY change. A connector's specification refers to the sections of this specification for the behaviour it does not change, and states its differences from the default behaviour against the rules and table rows of this specification. A table row is identified by the value of its first cell; a row whose first cell is empty continues the row above it and is identified by that value together with the value of its second cell. An override point is identified by the value of its *Area* cell in [§17.4](#174-override-points).

### 17.2. Connector obligations

1. The client class of a connector MUST include `sql:Client`, and its `query`, `queryRow`, `execute`, `batchExecute`, `call` and `close` methods MUST delegate to the library, so that [§3](#3-client) and [§8](#8-query) to [§12](#12-stored-procedure-calls) apply.
2. A connector gives the library the URL, the user and password, an optional data source class name, a map of driver options and the pool configuration of the client. These values form the pool key ([§4.6](#46-pool-selection)); pool properties the connector derives from its options are covered by it.
3. When the user does not give a `sql:ConnectionPool` value to the initializer, the connector MUST give the value returned by `sql:getGlobalConnectionPool` ([§4.5](#45-global-pool-configuration)).
4. A connector decides, separately for `execute` and `batchExecute`, whether generated keys are requested ([§10.2](#102-execution-result)), and MUST state its choice and the options that control it in its specification, including the option of rule 7 of [§10.4](#104-generated-keys) when it offers one.
5. A connector MAY check in its own code that the array of query templates of `batchExecute` is not empty; when it does, it MUST return an `sql:ApplicationError` without obtaining a connection, as the library does (rule 2 of [§11.2](#112-behaviour), rule 12 of [§17.3](#173-fixed-behaviour)).
6. A connector-defined typed value class MUST NOT have the name of a typed value class of this library; a connector-defined OUT parameter class MUST have a name that ends with `OutParameter` and is not `InOutParameter`; and a connector-defined INOUT parameter class MUST be named `InOutParameter` and hold its IN value in a field named `in`, because the library identifies the parameter kinds of connector-defined classes by class name (rule 2 of [§17.3](#173-fixed-behaviour)).
7. A connector that provides a `sql:SchemaClient` MUST implement it as specified in [§14](#14-metadata-retrieval).
8. A connector's specification MUST list the override points of [§17.4](#174-override-points) that it uses.

### 17.3. Fixed behaviour

A connector cannot change the following behaviour; it applies to every connector.

1. The SQL text sent to the driver is built from the query template as specified in [§5.3](#53-sql-text-construction).
2. The kind of each parameter of a `sql:ParameterizedCallQuery` is decided by the name of its class ([§12.2](#122-parameter-classification)). A typed value, OUT or INOUT parameter class of this library is identified by its module and class name, and a record value is bound or mapped as a `time` type only when its type is `Civil`, `Date` or `TimeOfDay` of the `ballerina/time` module ([§6](#6-parameter-binding), [§9.3](#93-target-types)). A user-defined or connector-defined type that has the same name as one of these types is not treated as it; a connector's own classes are recognised as the connector specifies.
3. The connection used by an operation, in a transaction and outside a transaction, and the release of statements and connections ([§13](#13-transactions) and the lifecycle rules of each operation).
4. The closed-client behaviour ([§3.3.2](#332-closed-state)).
5. The rules of `batchExecute` on its query templates, the chunk size and the content of `sql:BatchExecuteError` ([§11](#11-batch-execute)).
6. The detection of DDL statements for generated keys and the conversion of a generated key ([§10.4](#104-generated-keys)).
7. The lifecycle of `sql:ResultIterator`, and therefore of the streams returned by `query`, by `sql:ProcedureCallResult` and by `sql:CursorOutParameter` ([§8.3](#83-stream-lifecycle), [§8.4](#84-error-streams)).
8. The `close` method of `sql:ProcedureCallResult`, and the default `getNextQueryResult` ([§12.4](#124-procedure-call-result)).
9. Pool creation, validation of `sql:ConnectionPool` values, pool sharing and shutdown ([§4](#4-connection-pool)), and pool metrics ([§16](#16-observability)).
10. The error types of each failure category ([§15](#15-errors)). For a failure in behaviour it overrides ([§17.4](#174-override-points)), a connector returns the error type that this specification gives for that failure, or a subtype of it. A different type that a connector's specification already lists as a difference stays until the connector's next major version.
11. The matching of columns to record fields, including the `sql:Column` annotation and the grouping of columns into record fields ([§7.3](#73-column-matching), [§7.4](#74-grouping-columns-into-record-fields)), and the handling of unmatched columns and fields ([§7.5](#75-building-the-value), [§7.7](#77-unmatched-columns)). Of the column conversion rules of [§7.6](#76-column-conversion), the selection of a union member type and the conversion to a union of singleton types (rule 1), the conversion of SQL `NULL` (rule 6), and the check of field types before the first row (rule 6 of [§7.3](#73-column-matching)) are also fixed; a connector can change only the conversion of each column value (rules 2–5 of [§7.6](#76-column-conversion)).
12. The check that the array of query templates of `batchExecute` is not empty (rule 2 of [§11.2](#112-behaviour)).

### 17.4. Override points

A connector MAY change the following default behaviour.

| Area | What a connector can change | Default specified in |
|---|---|---|
| Binding of basic types, `time` values and arrays | The JDBC call, the SQL type and the accepted values with which they are bound. | [§6.3](#63-basic-types), [§6.4](#64-time-types), [§6.5](#65-arrays) |
| Binding of typed values | The binding of each typed value class of this library, including its array element type name and element conversion. | [§6.6](#66-typed-values), [§6.7](#67-typed-array-values), [§6.8](#68-deprecated-array-value) |
| Connector-defined typed values | The binding of its own typed value classes, and the SQL type an INOUT parameter with such an IN value is registered with. | [§6.2](#62-binding-procedure) rule 2.3, [§12.5](#125-out-and-inout-parameter-registration) |
| Record parameters | The binding of a value of a record type other than the `time` records. | [§6.2](#62-binding-procedure) rule 2.4 |
| Nil parameters | The SQL type with which `()` is bound. | [§6.2](#62-binding-procedure) rule 2.1 |
| OUT parameter registration | The SQL type registered for connector-defined OUT parameter classes, and how an OUT or INOUT parameter is registered with the statement. | [§12.5](#125-out-and-inout-parameter-registration) |
| Result conversion | The accepted target types and the conversion of a column value of each JDBC type, including database-specific SQL types, arrays and structured values. | [§7.6](#76-column-conversion) rules 2–5 |
| OUT and INOUT values | The reading and conversion of OUT and INOUT parameter values, including connector-defined OUT and INOUT parameter classes and arrays, and how the rows of a cursor parameter's stream are read and converted; the stream's lifecycle is fixed (rule 7 of [§17.3](#173-fixed-behaviour)). | [§12.6](#126-reading-out-and-inout-parameters), [§12.7](#127-out-parameter-type-mapping), [§12.8](#128-cursor-parameters) |
| Result iteration | A `sql:CustomResultIterator`. The `sql:ResultIterator` of every stream the connector creates then obtains each row from its `nextResult` method, and `getNextQueryResult` of a `sql:ProcedureCallResult` delegates to its `getNextQueryResult` method. | [§8.6](#86-interactions), [§12.4.2](#1242-moving-to-the-next-result) |
| Pool properties | Pool properties derived from connector options, other than the time an operation waits for a connection, which `connectionTimeout` alone sets. They are covered by the pool key ([§4.7](#47-pool-creation)). | [§4.7](#47-pool-creation) |
| Metric tags | The tags `db_host`, `db_port` and `db_name` of the pool metrics. | [§16.4](#164-tags) |
| Data source | The driver or data source class, including an XA data source. | [§4.7](#47-pool-creation), [§4.8](#48-xa-data-sources) |

### 17.5. Interactions

- Every value read through a connector's client uses the connector's result conversion: the rows of `query` and `queryRow`, the result sets of `call` held in `queryResult`, and the values returned by the `get` methods of OUT, INOUT and cursor parameters, including the OUT and INOUT parameter classes of this library, whether or not the connector supplies a `sql:CustomResultIterator`.
- A connector's compiler plugin can report the diagnostics of [§4.9](#49-compile-time-validation) for a pool configuration written in its client's constructor call.
- `sql:generateApplicationErrorStream` returns an error stream ([§8.4](#84-error-streams)), which a connector can return from a method that returns a stream.

## 18. Security considerations

1. Parameters of a query template are always bound as values and never become part of the SQL text ([§5.3](#53-sql-text-construction)). SQL text built by assigning the `strings` field of a query template, or from a `string` value in any other way, is sent to the database as given: the library does not validate or escape it, so text derived from untrusted input can change the statement that is executed.
2. A client that does not get a pool of its own can use a pool created by another client with an equal pool key ([§4.6](#46-pool-selection)). The user, the password and the data source class are part of the pool key, so a client never uses a pool opened with other credentials or another data source class (rule 6 of [§4.6](#46-pool-selection)).
3. The user and password are passed to the JDBC driver, and, when the connector gives a data source class name, also as the data source properties `user` and `password` ([§4.7](#47-pool-creation)). Error messages include the SQL text with `?` in place of each parameter, never the parameter values ([§15.3](#153-mapping)). Metric tags never contain the URL, the user or the password ([§16.4](#164-tags)).
4. The library does not configure transport encryption. Whether the connection to the database is encrypted is determined by the connector, the URL and the driver properties.
5. A stream ([§8.3](#83-stream-lifecycle)) or a `sql:ProcedureCallResult` ([§12.4.3](#1243-closing)) that is neither consumed nor closed keeps a connection out of its pool. When all connections of a pool are held, every other operation on the pool waits up to `connectionTimeout` and then fails ([§3.4](#34-concurrency)).
6. The library does not limit the number of rows or the size of values it reads. A row is read only when a stream's `next` is called, but a value is read into memory in full, and binary and character channels in typed array values are read into memory in full before binding ([§6.7](#67-typed-array-values)).
7. `readOnly` puts the connections of a pool in read-only mode as specified by the driver ([§4.2](#42-configuration)); it is not an access control mechanism of the library.
8. No log output of the connection pool implementation (HikariCP) or of the JDBC driver reaches the application's output, including the warnings of leak detection (`leakDetectionThreshold`, [§4.2](#42-configuration)). Whether suppressing it affects other loggers of the Java runtime is _implementation-defined_.

## 19. Planned changes

This section is informative. It lists behaviour changes planned for the next major version; the rest of this specification describes the current release.

| Change | Affected rule | Current behaviour | Planned behaviour | How to keep the current behaviour |
|---|---|---|---|---|
| Generated keys only for inserting statements | Rule 7 of [§10.4](#104-generated-keys) | When the connector enables generated keys, they are requested for every statement that is not DDL (`sql:GeneratedKeysMode` `ALL_STATEMENTS`); `INSERT_ONLY` is an option. | `INSERT_ONLY` is the default. | Set the connector's `sql:GeneratedKeysMode` option to `ALL_STATEMENTS`. |
| Equality of driver options maps in the pool key | Rule 2 of [§4.6](#46-pool-selection) | Java object equality: an absent and an empty options map, or `decimal` values that differ only in scale, give different pool keys. | An absent options map equals an empty one, and values are compared with Ballerina `==`, so such clients share a pool. | Give the clients that must not share a pool different `sql:ConnectionPool` values. |
| Untyped `()` INOUT value | `()` row of [§12.5](#125-out-and-inout-parameter-registration) | Registered with the JDBC type `NULL` by default, with a warning. | `call` returns an `sql:ApplicationError`. | Pass a typed value whose `value` is `()`, such as `new sql:IntegerValue()`. |
| Unreachable database at client creation | [§4.10](#410-errors), step 8 of [§4.7](#47-pool-creation) | `sql:ApplicationError` when `initializationFailTimeout` is greater than `0`, `sql:DatabaseError` otherwise. | `sql:DatabaseError` whatever `initializationFailTimeout` is. | Handle `sql:Error`, the supertype of both. |
| XA pool configuration | Rule 1 of [§4.8](#48-xa-data-sources) | Fields other than `maxOpenConnections` and `maxConnectionLifeTime` are ignored with a warning, and values are not validated. | The configuration is validated as for a regular pool, and a non-default field that an XA pool cannot apply results in an `sql:ApplicationError`. | Set only `maxOpenConnections` and `maxConnectionLifeTime`, with valid values. |
| A field matched by several columns | Rule 5 of [§7.3](#73-column-matching) | The last matching column gives the value. | For a closed record type, an `sql:FieldMismatchError` before the first row; open record types keep the last column. | Give the columns distinct aliases, or use an open record type. |
| Empty SQL array into a nilable target | Rule 3.3 of [§7.6](#76-column-conversion) | `()`. | An empty array. | Treat an empty array as `()` after reading. |
| Element types of `int?[]` and `float?[]` parameters | Rule 1 of [§6.5](#65-arrays) | `INTEGER` and `FLOAT`. | `BIGINT` and `DOUBLE`, as for the scalar values. | Bind with `sql:IntegerArrayValue` or `sql:FloatArrayValue`. |
| Classification of binding failures | Rule 2 of [§6.9](#69-errors) | By the text of the driver's message (driver-defined). | By SQLSTATE: class `22` gives an `sql:DataError`. | Handle both `sql:DataError` and `sql:DatabaseError`. |
| Fractional seconds of `time:Utc` and `TIME` | Rule 4 of [§6.4](#64-time-types), `TIME` and `time:Utc` rows of rule 2 of [§7.6](#76-column-conversion) | `time:Utc` is bound and read with millisecond precision; `TIME` read into `time:TimeOfDay` or `string` drops fractional seconds. | The full fraction is kept. | Truncate the values to the precision needed before binding them and after reading them. |
| `json` target of a character OUT parameter | `CHAR` row of [§12.7](#127-out-parameter-type-mapping) | The character value as a `json` string. | The value parsed as JSON, as in [§7.6](#76-column-conversion). | Read the value with `get(string)`. |

## References

- [JDBC 4.3 Specification (JSR 221)](https://jcp.org/en/jsr/detail?id=221)
- [`java.sql.Types`](https://docs.oracle.com/en/java/javase/21/docs/api/java.sql/java/sql/Types.html)
- [Ballerina language specification](https://ballerina.io/spec/lang/master/)
- [RFC 2119: Key words for use in RFCs to Indicate Requirement Levels](https://www.rfc-editor.org/rfc/rfc2119)
- [RFC 8174: Ambiguity of Uppercase vs Lowercase in RFC 2119 Key Words](https://www.rfc-editor.org/rfc/rfc8174)
