CREATE TABLE IF NOT EXISTS DataTable(
  row_id       INTEGER,
  int_type     INTEGER,
  long_type    BIGINT,
  float_type   FLOAT,
  double_type  DOUBLE,
  boolean_type BOOLEAN,
  string_type  VARCHAR(50),
  decimal_type DECIMAL(20, 2),
  PRIMARY KEY (row_id)
);

INSERT INTO DataTable (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
  VALUES(1, 1, 9223372036854774807, 123.34, 2139095039, TRUE, 'Hello', 23.45);

INSERT INTO DataTable (row_id) VALUES (2);

CREATE TABLE IF NOT EXISTS DataTableRep(
  row_id       INTEGER,
  int_type     INTEGER,
  PRIMARY KEY (row_id)
);

INSERT INTO DataTableRep (row_id, int_type) VALUES (1, 100);

INSERT INTO DataTableRep (row_id, int_type) VALUES (2, 200);

CREATE TABLE IF NOT EXISTS FloatTable(
  row_id       INTEGER,
  float_type   FLOAT,
  double_type  DOUBLE,
  numeric_type NUMERIC(10,2),
  decimal_type  DECIMAL(10,2),
  PRIMARY KEY (row_id)
);

INSERT INTO FloatTable (row_id, float_type, double_type, numeric_type, decimal_type) VALUES
  (1, 238999.34, 238999.34, 238999.34, 238999.34);

CREATE TABLE IF NOT EXISTS ComplexTypes(
  row_id         INTEGER NOT NULL,
  blob_type      BLOB(1024),
  clob_type      CLOB(1024),
  binary_type  BINARY(27),
  PRIMARY KEY (row_id)
);

INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type) VALUES
  (1, X'77736F322062616C6C6572696E6120626C6F6220746573742E', CONVERT('very long text', CLOB),
  X'77736F322062616C6C6572696E612062696E61727920746573742E');

INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type) VALUES
  (2, null, null, null);

CREATE TABLE IF NOT EXISTS ArrayTypes(
  row_id        INTEGER NOT NULL,
  int_array     INTEGER ARRAY,
  long_array    BIGINT ARRAY,
  float_array   FLOAT ARRAY,
  double_array  DOUBLE ARRAY,
  boolean_array BOOLEAN ARRAY,
  string_array  VARCHAR(20) ARRAY,
  PRIMARY KEY (row_id)
);

INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, boolean_array, string_array)
  VALUES (1, ARRAY [1, 2, 3], ARRAY [100000000, 200000000, 300000000], ARRAY[245.23, 5559.49, 8796.123],
  ARRAY[245.23, 5559.49, 8796.123], ARRAY[TRUE, FALSE, TRUE], ARRAY['Hello', 'Ballerina']);

INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, boolean_array, string_array)
  VALUES (2, ARRAY[NULL, 2, 3], ARRAY[100000000, NULL, 300000000], ARRAY[NULL, 5559.49, NULL],
  ARRAY[NULL, NULL, 8796.123], ARRAY[NULL , NULL, TRUE], ARRAY[NULL, 'Ballerina']);

INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, boolean_array, string_array)
  VALUES (3, NULL, NULL, NULL, NULL, NULL, NULL);

INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, boolean_array, string_array)
  VALUES (5, ARRAY[NULL, NULL, NULL], ARRAY[NULL, NULL, NULL], ARRAY[NULL, NULL, NULL],
  ARRAY[NULL, NULL, NULL], ARRAY[NULL , NULL, NULL], ARRAY[NULL, NULL]);

CREATE TABLE IF NOT EXISTS MixTypes (
  row_id INTEGER NOT NULL,
  int_type INTEGER,
  long_type BIGINT,
  float_type FLOAT,
  double_type DOUBLE,
  boolean_type BOOLEAN,
  string_type VARCHAR (50),
  json_type VARCHAR (50),
  decimal_type DECIMAL(16,4),
  int_array INTEGER ARRAY,
  long_array BIGINT ARRAY,
  float_array FLOAT ARRAY,
  double_array DOUBLE ARRAY,
  boolean_array BOOLEAN ARRAY,
  string_array VARCHAR (50) ARRAY,
  PRIMARY KEY (row_id)
);

INSERT INTO MixTypes (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, json_type, decimal_type,
  int_array, long_array, float_array, double_array, boolean_array, string_array)
VALUES (1, 1, 9223372036854774807, 123.34, 2139095039, TRUE, 'Hello', '[1, 2, 3]', 342452151425.4556, ARRAY[1, 2, 3],
  ARRAY[100000000, 200000000, 300000000], ARRAY[245.23, 5559.49, 8796.123],
  ARRAY[245.23, 5559.49, 8796.123], ARRAY[TRUE, FALSE, TRUE], ARRAY['Hello', 'Ballerina']);

CREATE TABLE IF NOT EXISTS DateTimeTypes(
  row_id         INTEGER NOT NULL,
  date_type      DATE,
  time_type      TIME,
  timestamp_type timestamp,
  datetime_type  datetime,
  time_tz_type      TIME WITH TIME ZONE,
  timestamp_tz_type TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (row_id)
);

INSERT INTO DateTimeTypes (row_id, date_type, time_type, timestamp_type,
            datetime_type, time_tz_type, timestamp_tz_type) VALUES (1, '2017-05-23', '14:15:23', '2017-01-25 16:33:55',
            '2017-01-25 16:33:55', '16:33:55+6:30', '2017-01-25 16:33:55-8:00');

CREATE TABLE IF NOT EXISTS IntegerTypes (
  id INTEGER,
  intData INTEGER,
  tinyIntData TINYINT,
  smallIntData SMALLINT,
  bigIntData BIGINT
);

CREATE TABLE IF NOT EXISTS DataTypeTableNillable(
  row_id       INTEGER,
  int_type     INTEGER,
  long_type    BIGINT,
  float_type   FLOAT,
  double_type  DOUBLE,
  boolean_type BOOLEAN,
  string_type  VARCHAR(50),
  numeric_type NUMERIC(10,3),
  decimal_type DECIMAL(10,3),
  real_type    REAL,
  tinyint_type TINYINT,
  smallint_type SMALLINT,
  clob_type    CLOB,
  blob_type    BLOB,
  binary_type  BINARY(27),
  date_type      DATE,
  time_type      TIME,
  datetime_type  DATETIME,
  timestamp_type TIMESTAMP,
  PRIMARY KEY (row_id)
);

CREATE TABLE IF NOT EXISTS DataTypeTableNillableBlob(
  row_id       INTEGER,
  blob_type    BLOB,
  PRIMARY KEY (row_id)
);

INSERT INTO DataTypeTableNillable (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type,
  numeric_type, decimal_type, real_type, tinyint_type, smallint_type, clob_type, blob_type, binary_type, date_type,
  time_type, datetime_type, timestamp_type) VALUES
  (1, 10, 9223372036854774807, 123.34, 2139095039, TRUE, 'Hello',1234.567, 1234.567, 1234.567, 1, 5555,
  CONVERT('very long text', CLOB), X'77736F322062616C6C6572696E6120626C6F6220746573742E',
  X'77736F322062616C6C6572696E612062696E61727920746573742E', '2017-02-03', '11:35:45', '2017-02-03 11:53:00',
  '2017-02-03 11:53:00');

INSERT INTO DataTypeTableNillable (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type,
  numeric_type, decimal_type, real_type, tinyint_type, smallint_type, clob_type, blob_type, binary_type, date_type,
  time_type, datetime_type, timestamp_type) VALUES
  (2, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);

INSERT INTO DataTypeTableNillableBlob (row_id, blob_type) VALUES
  (3, X'77736F322062616C6C6572696E6120626C6F6220746573742E');

INSERT INTO DataTypeTableNillableBlob (row_id, blob_type) VALUES (4, null);

CREATE TABLE IF NOT EXISTS Person(
  id       INTEGER,
  age      INTEGER,
  salary   FLOAT,
  name  VARCHAR(50),
  PRIMARY KEY (id)
);


INSERT INTO Person (id, age, salary, name) VALUES (1, 25, 400.25, 'John');

INSERT INTO Person (id, age, salary, name) VALUES (2, 35, 600.25, 'Anne');

INSERT INTO Person (id, age, salary, name) VALUES (3, 45, 600.25, 'Mary');

INSERT INTO Person (id, age, salary, name) VALUES (10, 22, 100.25, 'Peter');

CREATE TABLE IF NOT EXISTS ArrayTypes2 (
  row_id        INTEGER NOT NULL,
  smallint_array SMALLINT ARRAY,
  int_array     INTEGER ARRAY,
  long_array    BIGINT ARRAY,
  float_array   FLOAT ARRAY,
  double_array  DOUBLE ARRAY,
  real_array  REAL ARRAY,
  decimal_array  DECIMAL(6,2) ARRAY,
  numeric_array    NUMERIC(6,2) ARRAY,
  boolean_array BOOLEAN ARRAY,
  bit_array BIT(4) ARRAY,
  char_array CHAR(15) ARRAY,
  varchar_array VARCHAR(100) ARRAY,
  nvarchar_array NVARCHAR(15) ARRAY,
  string_array  VARCHAR(20) ARRAY,
  blob_array    VARBINARY(27) ARRAY,
  date_array DATE ARRAY,
  time_array TIME ARRAY,
  datetime_array DATETIME ARRAY,
  timestamp_array timestamp ARRAY,
  time_tz_array  TIME WITH TIME ZONE ARRAY,
  timestamp_tz_array TIMESTAMP WITH TIME ZONE ARRAY,
  PRIMARY KEY (row_id)
);

INSERT INTO ArrayTypes2 (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array, bit_array, string_array, blob_array, smallint_array, numeric_array, real_array, char_array, varchar_array, nvarchar_array, date_array, time_array, datetime_array, timestamp_array, time_tz_array, timestamp_tz_array)
  VALUES (1, ARRAY [1, 2, 3], ARRAY [100000000, 200000000, 300000000], ARRAY[245.23, 5559.49, 8796.123],
  ARRAY[245.23, 5559.49, 8796.123], ARRAY[245.12, 5559.12, 8796.92], ARRAY[TRUE, FALSE, TRUE], ARRAY[B'001', B'011', B'100'], ARRAY['Hello', 'Ballerina'],
  ARRAY[X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E'], ARRAY[12, 232], ARRAY[11.11, 23.23], ARRAY[199.33, 2399.1], ARRAY['Hello', 'Ballerina'], ARRAY['Hello', 'Ballerina'], ARRAY['Hello', 'Ballerina'], ARRAY['2017-02-03', '2017-02-03'], ARRAY['11:22:42', '12:23:45'], ARRAY['2017-02-03 11:53:00', '2019-04-05 12:33:10'], ARRAY['2017-02-03 11:53:00', '2019-04-05 12:33:10'],
  ARRAY['16:33:55+6:30', '16:33:55+4:30'], ARRAY['2017-01-25 16:33:55-8:00', '2017-01-25 16:33:55-5:00']);

INSERT INTO ArrayTypes2 (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array, bit_array, string_array, blob_array, smallint_array, numeric_array, real_array, char_array, varchar_array, nvarchar_array, date_array, time_array, datetime_array, timestamp_array, time_tz_array, timestamp_tz_array)
  VALUES (2, ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null],
  ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null], ARRAY[Null, Null]);

INSERT INTO ArrayTypes2 (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array, bit_array, string_array, blob_array, smallint_array, numeric_array, real_array, char_array, varchar_array, nvarchar_array, date_array, time_array, datetime_array, timestamp_array, time_tz_array, timestamp_tz_array)
  VALUES (3, ARRAY[Null, 1, 2, 3], ARRAY[Null, 100000000, 200000000, 300000000], ARRAY[Null, 245.23, 5559.49, 8796.123],
  ARRAY[Null, 245.23, 5559.49, 8796.123], ARRAY[Null, 245.12, 5559.12, 8796.92], ARRAY[Null, TRUE, FALSE, TRUE], ARRAY[Null, B'001', B'011', B'100'], ARRAY[Null, 'Hello', 'Ballerina'],
  ARRAY[Null, X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E'], ARRAY[Null, 12, 232], ARRAY[Null, 11.11, 23.23], ARRAY[Null, 199.33, 2399.1], ARRAY[Null, 'Hello', 'Ballerina'], ARRAY[Null, 'Hello', 'Ballerina'], ARRAY[Null, 'Hello', 'Ballerina'], ARRAY[Null, '2017-02-03', '2017-02-03'], ARRAY[Null, '11:22:42', '12:23:45'], ARRAY[Null, '2017-02-03 11:53:00', '2019-04-05 12:33:10'], ARRAY[Null, '2017-02-03 11:53:00', '2019-04-05 12:33:10'],
  ARRAY[null, '16:33:55+6:30', '16:33:55+4:30'], ARRAY[null, '2017-01-25 16:33:55-8:00', '2017-01-25 16:33:55-5:00']);

INSERT INTO ArrayTypes2 (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array, bit_array, string_array, blob_array, smallint_array, numeric_array, real_array, char_array, varchar_array, nvarchar_array, date_array, time_array, datetime_array, timestamp_array, time_tz_array, timestamp_tz_array)
  VALUES (4, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null, Null);

CREATE TYPE INT_TYPE AS INT;
CREATE TYPE BOOLEAN_TYPE AS BOOLEAN;
CREATE TYPE STRING_TYPE AS VARCHAR(100);
CREATE TYPE FLOAT_TYPE AS FLOAT;

CREATE TABLE IF NOT EXISTS UserDefinedTypesTable (
    udt_int INT_TYPE NOT NULL,
    udt_boolean BOOLEAN_TYPE,
    udt_string STRING_TYPE,
    udt_float FLOAT_TYPE,
    PRIMARY KEY (udt_int)
);

INSERT INTO UserDefinedTypesTable (udt_int, udt_boolean, udt_string, udt_float) VALUES
  (1, true, 'User defined type string', 12.32);
