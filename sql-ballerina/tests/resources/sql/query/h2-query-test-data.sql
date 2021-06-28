CREATE TABLE IF NOT EXISTS OtherTypes (
                            id INT IDENTITY,
                            clob_type    CLOB(1024),
                            blob_type    BLOB(1024),
                            var_binary_type VARBINARY(27),
                            int_array_type ARRAY,
                            string_array_type ARRAY,
                            binary_type  BINARY(27),
                            boolean_type BOOLEAN,
                            PRIMARY KEY (id)
                    );

INSERT INTO OtherTypes(id, clob_type, blob_type, var_binary_type, int_array_type, string_array_type, binary_type, boolean_type)
                    VALUES (1, CONVERT('very long text', CLOB), X'77736F322062616C6C6572696E6120626C6F6220746573742E',
                    X'77736F322062616C6C6572696E612062696E61727920746573742E', ARRAY [1, 2, 3], ARRAY['Hello', 'Ballerina'],
                    X'77736F322062616C6C6572696E612062696E61727920746573742E', TRUE);
