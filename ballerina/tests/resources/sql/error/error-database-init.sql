CREATE TABLE IF NOT EXISTS DataTable(
    row_id       INTEGER,
    string_type  VARCHAR(50),
    PRIMARY KEY (row_id)
);

INSERT INTO DataTable (row_id, string_type) VALUES(1, '{""q}');