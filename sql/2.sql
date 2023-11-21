WITH
  CLASSICGames AS (
   SELECT
     Opening
   , Result
   FROM
     test1.data_source_12
   WHERE (LOWER(Event) LIKE '%classic%')
) 
, OpeningCounts AS (
   SELECT
     Opening
   , COUNT(*) TotalGames
   , SUM((CASE WHEN (Result = '1-0') THEN 1 ELSE 0 END)) WhiteWins
   , SUM((CASE WHEN (Result = '0-1') THEN 1 ELSE 0 END)) BlackWins
   FROM
     CLASSICGames 
   GROUP BY Opening
) 
SELECT
  Opening
, TotalGames
, ROUND(((WhiteWins * 1E2) / TotalGames), 2) WhiteWinRate
, ROUND(((BlackWins * 1E2) / TotalGames), 2) BlackWinRate
FROM
  OpeningCounts
ORDER BY TotalGames DESC
LIMIT 10