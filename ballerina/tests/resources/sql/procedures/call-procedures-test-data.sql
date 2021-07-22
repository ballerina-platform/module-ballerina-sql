CREATE TABLE IF NOT EXISTS StringTypes (
                             id INT IDENTITY,
                             varchar_type VARCHAR(255),
                             charmax_type CHAR(10),
                             char_type CHAR,
                             charactermax_type CHARACTER(10),
                             character_type CHARACTER,
                             nvarcharmax_type NVARCHAR(255),
                             PRIMARY KEY (id)
                    );

INSERT INTO StringTypes(id, varchar_type, charmax_type, char_type, charactermax_type, character_type, nvarcharmax_type)
                    VALUES (1, 'test0', 'test1', 'a', 'test2', 'b', 'test3'); 

CREATE TABLE IF NOT EXISTS OtherTypes (
                            id INT IDENTITY,
                            clob_type    CLOB,
                            blob_type    BLOB,
                            var_binary_type VARBINARY(27),
                            int_array_type INT ARRAY,
                            string_array_type VARCHAR(50) ARRAY,
                            binary_type  BINARY(27),
                            boolean_type BOOLEAN,
                            PRIMARY KEY (id)
                    );

INSERT INTO OtherTypes(id, clob_type, blob_type, var_binary_type, int_array_type, string_array_type, binary_type, boolean_type)
                    VALUES (1, CONVERT('very long text', CLOB), X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                    X'77736F322062616C6C6572696E612062696E61727920746573742E', ARRAY [1, 2, 3], ARRAY['Hello', 'Ballerina'],
                    X'77736F322062616C6C6572696E612062696E61727920746573742E', TRUE);

CREATE TABLE IF NOT EXISTS NumericTypes (
                              id INT IDENTITY,
                              int_type INT,
                              bigint_type BIGINT,
                              smallint_type SMALLINT,
                              tinyint_type TINYINT,
                              bit_type BIT,
                              decimal_type DECIMAL(10,2),
                              numeric_type NUMERIC(10,2),
                              float_type FLOAT,
                              real_type REAL,
                              double_type DOUBLE,
                              PRIMARY KEY (id)
);


INSERT INTO NumericTypes (id, int_type, bigint_type, smallint_type, tinyint_type, bit_type, decimal_type, numeric_type,
                          float_type, real_type, double_type)
                  VALUES (1, 2147483647, 9223372036854774807, 32767, 127, 1, 1234.56, 1234.56,1234.56, 1234.56, 1234.56); 

CREATE TABLE IF NOT EXISTS StringTypesSecond (
                             id INT IDENTITY,
                             varchar_type VARCHAR(255),
                             charmax_type CHAR(10),
                             char_type CHAR,
                             charactermax_type CHARACTER(10),
                             character_type CHARACTER,
                             nvarcharmax_type NVARCHAR(255),
                             PRIMARY KEY (id)
                    );
CREATE TABLE IF NOT EXISTS DateTimeTypes (
                             id INT IDENTITY,
                             date_type DATE,
                             time_type TIME,
                             datetime_type DATETIME,
                             timewithtz_type TIME WITH TIME ZONE,
                             timestamp_type TIMESTAMP,
                             timestampwithtz_type TIMESTAMP WITH TIME ZONE,
                             PRIMARY KEY (id)
                    );

INSERT INTO DateTimeTypes (id, date_type, time_type, datetime_type, timestamp_type, timewithtz_type, timestampwithtz_type)
 VALUES (1, '2017-05-23', '14:15:23', '2017-01-25 16:33:55', '2017-01-25 16:33:55', '16:33:55+6:30', '2017-01-25 16:33:55-8:00');

CREATE TABLE IF NOT EXISTS MultipleRecords (
                            id INT IDENTITY,
                            name VARCHAR(255),
                            age INT,
                            birthday DATE,
                            country_code VARCHAR(10),
                            PRIMARY KEY (id)
);

INSERT INTO MultipleRecords (id, name, age, birthday, country_code)
    VALUES(1, 'Bob', 20, '2017-05-23', 'US');
INSERT INTO MultipleRecords (id, name, age, birthday, country_code)
    VALUES(2, 'John', 25, '2012-10-12', 'US');

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

CREATE TABLE IF NOT EXISTS ProArrayTypes (
  row_id        INTEGER NOT NULL,
  smallint_array SMALLINT ARRAY,
  int_array     INTEGER ARRAY,
  long_array    BIGINT ARRAY,
  float_array   FLOAT ARRAY,
  double_array  DOUBLE ARRAY,
  real_array  REAL ARRAY,
  decimal_array  DECIMAL(6,2) ARRAY,
  numeric_array NUMERIC(6,2) ARRAY,
  boolean_array BOOLEAN ARRAY,
  bit_array BIT ARRAY,
  binary_array BINARY(27) ARRAY,
  char_array CHAR(15) ARRAY,
  varchar_array VARCHAR(100) ARRAY,
  nvarchar_array NVARCHAR(15) ARRAY,
  string_array VARCHAR(20) ARRAY,
  blob_array VARBINARY(27) ARRAY,
  date_array DATE ARRAY,
  time_array TIME ARRAY,
  datetime_array DATETIME ARRAY,
  timestamp_array timestamp ARRAY,
  time_tz_array  TIME WITH TIME ZONE ARRAY,
  timestamp_tz_array TIMESTAMP WITH TIME ZONE ARRAY,
  PRIMARY KEY (row_id)
);

INSERT INTO ProArrayTypes (row_id, int_array, long_array, float_array, double_array, decimal_array, boolean_array,
bit_array, string_array, blob_array, smallint_array, numeric_array, real_array, char_array, varchar_array,
nvarchar_array, date_array, time_array, datetime_array, timestamp_array, time_tz_array, timestamp_tz_array, binary_array)
  VALUES (1, ARRAY [1, 2, 3], ARRAY [100000000, 200000000, 300000000], ARRAY[245.23, 5559.49, 8796.123],
  ARRAY[245.23, 5559.49, 8796.123], ARRAY[245.12, 5559.12, 8796.92], ARRAY[TRUE, FALSE, TRUE],
  ARRAY[1, 1, 0], ARRAY['Hello', 'Ballerina'],
  ARRAY[X'77736F322062616C6C6572696E6120626C6F6220746573742E', X'77736F322062616C6C6572696E6120626C6F6220746573742E'],
  ARRAY[12, 232], ARRAY[11.11, 23.23], ARRAY[199.33, 2399.1], ARRAY['Hello', 'Ballerina'], ARRAY['Hello', 'Ballerina'],
  ARRAY['Hello', 'Ballerina'], ARRAY['2017-02-03', '2017-02-03'], ARRAY['11:22:42', '12:23:45'],
  ARRAY['2017-02-03 11:53:00', '2019-04-05 12:33:10'], ARRAY['2017-02-03 11:53:00', '2019-04-05 12:33:10'],
  ARRAY['16:33:55+6:30', '16:33:55+4:30'], ARRAY['2017-01-25 16:33:55-8:00', '2017-01-25 16:33:55-5:00'],
  ARRAY[X'77736F322062616C6C6572696E612062696E61727920746573747R']);
