# Change Log
This file contains all the notable changes done to the Ballerina SQL package through the releases.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed
- [Add support for queryRow](https://github.com/ballerina-platform/ballerina-standard-library/issues/1604)
- [Add completion type as nil in SQL query return stream type](https://github.com/ballerina-platform/ballerina-standard-library/issues/1654)
- [Implement array types for call procedure](https://github.com/ballerina-platform/ballerina-standard-library/issues/1516)

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
