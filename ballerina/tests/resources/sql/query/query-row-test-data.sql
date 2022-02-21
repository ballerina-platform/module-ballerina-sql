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

INSERT INTO DataTable (row_id, int_type, long_type, float_type, double_type, boolean_type, string_type, decimal_type)
  VALUES(3, 1, 9372036854774807, 124.34, 29095039, false, '1', 25.45);

CREATE TABLE IF NOT EXISTS ComplexTypes(
  row_id         INTEGER NOT NULL,
  blob_type      BLOB(1024),
  clob_type      CLOB(1024),
  binary_type  BINARY(27),
  var_binary_type VARBINARY(27),
  uuid_type UUID,
  other_type OTHER,
  PRIMARY KEY (row_id)
);

INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type, var_binary_type, uuid_type) VALUES
  (1, X'77736F322062616C6C6572696E6120626C6F6220746573742E', CONVERT('very long text', CLOB),
  X'77736F322062616C6C6572696E612062696E61727920746573742E', X'77736F322062616C6C6572696E612062696E61727920746573742E',
  '24ff1824-01e8-4dac-8eb3-3fee32ad2b9c');

INSERT INTO ComplexTypes (row_id, blob_type, clob_type, binary_type, var_binary_type) VALUES
  (2, null, null, null, null);


CREATE TABLE NumericTypes (
   id INT IDENTITY,
   int_type INT NOT NULL,
   bigint_type BIGINT NOT NULL,
   smallint_type SMALLINT NOT NULL ,
   tinyint_type TINYINT NOT NULL ,
   bit_type BIT NOT NULL ,
   decimal_type DECIMAL(10,3) NOT NULL ,
   numeric_type NUMERIC(10,3) NOT NULL ,
   float_type FLOAT NOT NULL ,
   real_type REAL NOT NULL ,
   PRIMARY KEY (id)
);

INSERT INTO NumericTypes (id, int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type, numeric_type,
    float_type, real_type) VALUES (1, 2147483647, 9223372036854774807, 32767, 127, 1, 1234.567, 1234.567, 1234.567,
    1234.567);


INSERT INTO NumericTypes (id, int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type, numeric_type,
    float_type, real_type) VALUES (2, 2147483647, 9223372036854774807, 32767, 127, 1, 1234, 1234, 1234,
    1234);

CREATE TABLE IF NOT EXISTS DateTimeTypes(
  row_id         INTEGER NOT NULL,
  date_type      DATE,
  time_type      TIME,
  timestamp_type TIMESTAMP,
  datetime_type  datetime,
  time_tz_type      TIME WITH TIME ZONE,
  timestamp_tz_type TIMESTAMP WITH TIME ZONE,
  PRIMARY KEY (row_id)
);

INSERT INTO DateTimeTypes (row_id, date_type, time_type, datetime_type, timestamp_type, time_tz_type, timestamp_tz_type) VALUES
  (1,'2017-02-03', '11:35:45', '2017-02-03 11:53:00', '2017-02-03 11:53:00','20:08:08-8:00','2008-08-08 20:08:08+8:00');

CREATE TABLE IF NOT EXISTS ArrayTypes(
  row_id        INTEGER NOT NULL,
  int_array     INTEGER ARRAY,
  long_array    BIGINT ARRAY,
  float_array   FLOAT ARRAY,
  double_array  DOUBLE ARRAY,
  decimal_array  DECIMAL ARRAY,
  boolean_array BOOLEAN ARRAY,
  string_array  VARCHAR(20) ARRAY,
  blob_array    VARBINARY(27) ARRAY,
  PRIMARY KEY (row_id)
);

INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array, string_array, blob_array)
  VALUES (1, ARRAY [1, 2, 3], ARRAY [10000, 20000, 30000], ARRAY[245.23, 5559.49, 8796.123],
  ARRAY[245.23, 5559.49, 8796.123], ARRAY[245, 5559, 8796], ARRAY[TRUE, FALSE, TRUE], ARRAY['Hello', 'Ballerina'],
  ARRAY[X'77736F322062616C6C6572696E6120626C6F6220746573742E']);

INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array,  decimal_array, boolean_array, string_array, blob_array)
  VALUES (2, ARRAY[NULL, 2, 3], ARRAY[100000000, NULL, 300000000], ARRAY[NULL, 5559.49, NULL],
  ARRAY[NULL, NULL, 8796.123], ARRAY[NULL, NULL, 8796], ARRAY[NULL , NULL, TRUE], ARRAY[NULL, 'Ballerina'],
  ARRAY[NULL, X'77736F322062616C6C6572696E6120626C6F6220746573742E']);

INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array, string_array, blob_array)
  VALUES (3, NULL, NULL, NULL, NULL,NULL , NULL, NULL, NULL);

INSERT INTO ArrayTypes (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array, string_array, blob_array)
  VALUES (5, ARRAY[NULL, NULL, NULL], ARRAY[NULL, NULL, NULL], ARRAY[NULL, NULL, NULL],
  ARRAY[NULL, NULL, NULL], ARRAY[NULL , NULL, NULL], ARRAY[NULL , NULL, NULL], ARRAY[NULL, NULL], ARRAY[NULL, NULL]);

CREATE TYPE MASK_ROW_TYPE AS INTEGER;

CREATE TABLE IF NOT EXISTS MaskTable(
  row_id       MASK_ROW_TYPE,
  int_type     INTEGER,
  PRIMARY KEY (row_id)
);

INSERT INTO MaskTable (row_id, int_type) VALUES(1, 1);

CREATE TABLE IF NOT EXISTS EmptyDataTable(
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

CREATE TABLE Teachers (
  id INT IDENTITY,
  name VARCHAR(45),
  PRIMARY KEY(id)
);

CREATE TABLE Students (
  id INT IDENTITY,
  name VARCHAR(45),
  age INTEGER,
  supervisorId INTEGER,
  PRIMARY KEY(id),
  FOREIGN KEY (supervisorId) REFERENCES Teachers(id)
);

INSERT INTO Teachers (id, name) VALUES (1, 'James');
INSERT INTO Students (id, name, age, supervisorId) VALUES (1, 'Alice', 25, 1);

CREATE TABLE Album (
    id_test     VARCHAR(10),
    name        VARCHAR(10),
    artist_test VARCHAR(10)
);

INSERT INTO Album VALUES ('1', 'Lemonade', 'Beyonce');
