CREATE USER generalUser PASSWORD 'password';

CREATE TABLE IF NOT EXISTS Customers (
  customerId INTEGER NOT NULL IDENTITY,
  firstName  VARCHAR(300) NOT NULL,
  lastName  VARCHAR(300) NOT NULL,
  registrationID INTEGER NOT NULL,
  creditLimit DOUBLE DEFAULT 100.00,
  country  VARCHAR(300),
  PRIMARY KEY (customerId),
  CHECK (creditLimit > 50)
);

CREATE TABLE IF NOT EXISTS DataTable (
  row_id       INTEGER IDENTITY,
  int_type     INTEGER,
  long_type    BIGINT,
  float_type   FLOAT,
  double_type  DOUBLE,
  boolean_type BOOLEAN,
  string_type  VARCHAR(50) DEFAULT 'test',
  decimal_type DECIMAL(20, 2),
  PRIMARY KEY (row_id)
);

CREATE TABLE NumericTypes (
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
   PRIMARY KEY (id)
);

CREATE VIEW IF NOT EXISTS CustomerNames AS SELECT firstName, lastName FROM Customers;

CREATE TABLE IF NOT EXISTS Company (
  companyId INTEGER NOT NULL IDENTITY,
  name VARCHAR(300) NOT NULL
);

CREATE TABLE IF NOT EXISTS Person (
  personId INTEGER NOT NULL IDENTITY,
  name VARCHAR(300) NOT NULL,
  companyId INTEGER NOT NULL,
  FOREIGN KEY (companyId) REFERENCES Company(companyId),
  CHECK (personId > 100),
  CHECK (companyId > 20)
);

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
