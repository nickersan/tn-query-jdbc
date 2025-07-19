-- noinspection SqlResolveForFile

CREATE TABLE Target
(
    boolean_value         BOOLEAN          NULL,
    byte_value            TINYINT          NULL,
    char_value            CHAR(1)          NULL,
    date_value            TIMESTAMP        NULL,
    double_value          DOUBLE PRECISION NULL,
    float_value           FLOAT(1)         NULL,
    int_value             INTEGER          NULL,
    local_date_value      DATE             NULL,
    local_date_time_value TIMESTAMP        NULL,
    long_value            BIGINT           NULL,
    short_value           SMALLINT         NULL,
    string_value          VARCHAR(1024)    NULL
)