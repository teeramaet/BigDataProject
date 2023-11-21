SELECT
  UTCDate
, COUNT(*) NumberOfGames
FROM
  test1.data_source_12
WHERE ((YEAR(UTCDate) = 2016) AND (MONTH(UTCDate) = 7))
GROUP BY UTCDate
ORDER BY UTCDate ASC