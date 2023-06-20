# Change Log
This file contains all the notable changes done to the Ballerina SQL package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

### Changed
- [Improve documentation regard `sql:Column` annotation](https://github.com/ballerina-platform/ballerina-standard-library/issues/4134)
- [Handle null error messages from underlying drivers](https://github.com/ballerina-platform/ballerina-standard-library/issues/4200)
- [Make `sql:Client` isolated](https://github.com/ballerina-platform/ballerina-standard-library/issues/4455)

## [1.7.1] - 2023-03-09

### Changed

- [Optimise batchExecute with a batch size of 1000](https://github.com/ballerina-platform/ballerina-standard-library/issues/4129)

## [1.7.0] - 2023-02-20

### Changed

- [Remove SQL_901 diagnostic hint](https://github.com/ballerina-platform/ballerina-standard-library/issues/3609)
- [Enable non-Hikari logs](https://github.com/ballerina-platform/ballerina-standard-library/issues/3763)

## [1.6.2] - 2023-02-09

### Changed

- [Improve API docs based on Best practices](https://github.com/ballerina-platform/ballerina-standard-library/issues/3857)
- [Fix SQL compiler plugins failure when the diagnostic code is null](https://github.com/ballerina-platform/ballerina-standard-library/issues/4054)

## [1.6.1] - 2022-12-22

### Changed
- [Enable non-Hikari logs](https://github.com/ballerina-platform/ballerina-standard-library/issues/3763)

## [1.6.0] - 2022-11-29

### Changed
- [Updated API Docs](https://github.com/ballerina-platform/ballerina-standard-library/issues/3463)
- [Fix unable to set unlimited lifetime (0) to ballerina.sql.maxConnectionLifeTime](https://github.com/ballerina-platform/ballerina-standard-library/issues/3657)
- [Improve error message on client connection failure](https://github.com/ballerina-platform/ballerina-standard-library/issues/3648)

## [1.5.0] - 2022-09-08

### Added
- [Added support for metadata retrieval](https://github.com/ballerina-platform/ballerina-standard-library/issues/3061)

### Changed
- [Fix sql:queryConcat not working for empty query](https://github.com/ballerina-platform/ballerina-standard-library/issues/3127)
- [Fix error not being thrown when mandatory fields are not fetched](https://github.com/ballerina-platform/ballerina-standard-library/issues/3251)

## [1.4.1] - 2022-06-21

### Changed
- [Fix NullPointerException when retrieving record with default value](https://github.com/ballerina-platform/ballerina-standard-library/issues/2985)

## [1.4.0] - 2022-05-30

### Added
- [Improve Database columns to Ballerina record Mapping through `sql:Column` Annotation](https://github.com/ballerina-platform/ballerina-standard-library/issues/2652)

### Changed
- [Fixed compiler plugin validation for `time` module constructs](https://github.com/ballerina-platform/ballerina-standard-library/issues/2893)

## [1.3.1] - 2022-03-01

### Changed
- Improve API documentation

## [1.3.0] - 2022-01-29

### Changed
- [Fix Compiler plugin crash when variable is passed for `sql:connectionPool`](https://github.com/ballerina-platform/ballerina-standard-library/issues/2536)

## [1.2.1] - 2022-02-03

### Changed
- [Fix Compiler plugin crash when variable is passed for `sql:connectionPool`](https://github.com/ballerina-platform/ballerina-standard-library/issues/2536)

## [1.2.0] - 2021-12-13

### Changed
- [Fix queryRow method to use module's provided resultParameterProcessor](https://github.com/ballerina-platform/ballerina-standard-library/issues/2466)

## [1.1.0] - 2021-11-20

### Added
- [Tooling support for SQL client](https://github.com/ballerina-platform/ballerina-standard-library/issues/2058)

### Changed

- [Accept escaped backtick as insertions in parameterised query](https://github.com/ballerina-platform/ballerina-standard-library/issues/2056)
- [Add support for handling union types in queryRow()](https://github.com/ballerina-platform/ballerina-standard-library/issues/2333)
- [Validate connection pool configurations](https://github.com/ballerina-platform/ballerina-standard-library/issues/2355)
- [Change queryRow return type to anydata](https://github.com/ballerina-platform/ballerina-standard-library/issues/2390)
- [Make OutParameter get function parameter optional](https://github.com/ballerina-platform/ballerina-standard-library/issues/2388)

## [1.0.0] - 2021-10-09

### Added

- [Add support for queryRow](https://github.com/ballerina-platform/ballerina-standard-library/issues/1604)
- [Add support for async operation](https://github.com/ballerina-platform/ballerina-standard-library/issues/120)
- [Implement array types for call procedure](https://github.com/ballerina-platform/ballerina-standard-library/issues/1516)
- [Add support for passing time:Date and time:TimeOfDay types directly into parameterized queries](https://github.com/ballerina-platform/ballerina-standard-library/issues/1891)
- [Add support for passing time:UTC type directly into parameterized queries](https://github.com/ballerina-platform/ballerina-standard-library/issues/1800)
- [Add support for passing time:Civil type directly into parameterized queries](https://github.com/ballerina-platform/ballerina-standard-library/issues/1799)
- [Add support for retrieving time:Utc type](https://github.com/ballerina-platform/ballerina-standard-library/issues/1909)
- [Introduce util function to concatenate queries](https://github.com/ballerina-platform/ballerina-standard-library/issues/1886)
- [Introduce arrayFlattenQuery() function to add IN operator values by using concatenate queries function](https://github.com/ballerina-platform/ballerina-standard-library/issues/1886)
- [Add support for retrieving time:Civil, time:Date and Time:TimeOfDay types as values using queryRow](https://github.com/ballerina-platform/ballerina-standard-library/issues/1939)
- [Add functionality to map multiple fields in the return query result to a single typed record field](https://github.com/ballerina-platform/ballerina-standard-library/issues/1924)

### Changed

- [Remove support for string parameter in SQL APIs](https://github.com/ballerina-platform/ballerina-standard-library/issues/2010)
- [Improve Errors](https://github.com/ballerina-platform/ballerina-standard-library/issues/1758)
- [Add completion type as nil in SQL query return stream type](https://github.com/ballerina-platform/ballerina-standard-library/issues/1654)
- [Fix type cast error for query api containing database error](https://github.com/ballerina-platform/ballerina-standard-library/issues/1759)
- [Fix null pointer exception being thrown when calling stored procedure without specifying return types](https://github.com/ballerina-platform/ballerina-standard-library/issues/1982)

## [0.6.0-beta.2] - 2021-06-22

### Changed
- [Change default rowType of the query remote method from `nil` to `<>`](https://github.com/ballerina-platform/ballerina-standard-library/issues/1445)

## [0.6.0-beta.1] - 2021-06-02

### Changed
- Add DB connection active status check in native code level
- [Remove sending `lastInsertId` in `ExecutionResult` for remote call function](https://github.com/ballerina-platform/ballerina-standard-library/issues/1409)

### Added
- [Introduced `ArrayValueType` type and `TypedValue` object to configure the types of an SQL array](https://github.com/ballerina-platform/ballerina-standard-library/issues/104)
- Newly introduce TimeWithTimezoneOutParameter and TimestampWithTimezoneOutParameter for using TimeWithTimeZone Type OutParameter
