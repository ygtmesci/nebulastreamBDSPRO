CREATE STREAM Numbers (
    value INT
) FROM CSV_FILE(
    path='numbers.csv',
    hasHeader=false
);

SELECT
    value
FROM Numbers;
