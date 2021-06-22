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
                            PRIMARY KEY (id)
                    );

INSERT INTO OtherTypes(id, clob_type, blob_type, var_binary_type, int_array_type, string_array_type)
                    VALUES (1, CONVERT('very long text', CLOB), X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                    X'77736F322062616C6C6572696E612062696E61727920746573742E', ARRAY [1, 2, 3], ARRAY['Hello', 'Ballerina']);

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
                             timewithtz_type TIME WITH TIME ZONE,
                             timestamp_type TIMESTAMP,
                             timestampwithtz_type TIMESTAMP WITH TIME ZONE,
                             PRIMARY KEY (id)
                    );

INSERT INTO DateTimeTypes (id, date_type, time_type, timestamp_type, timewithtz_type, timestampwithtz_type)
 VALUES (1, '2017-05-23', '14:15:23', '2017-01-25 16:33:55', '16:33:55+6:30', '2017-01-25 16:33:55-8:00');

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
