

WITH date_series AS (
    SELECT day_date
    FROM UNNEST(
        SEQUENCE(
            DATE '2016-01-01',
            DATE '2018-12-31',
            INTERVAL '1' DAY
        )
    ) AS t(day_date)
)

SELECT
    CAST(date_format(day_date, '%Y%m%d') AS INTEGER) AS date_key,
    day_date AS date,
    year(day_date) AS year,
    month(day_date) AS month,
    day(day_date) AS day,
    day_of_week(day_date) AS day_of_week,
    day_of_year(day_date) AS day_of_year
FROM date_series