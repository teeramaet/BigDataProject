WITH
  GameOutcomes AS (
   SELECT
     SUM((CASE WHEN (result = '1-0') THEN 1 ELSE 0 END)) WhiteWins
   , SUM((CASE WHEN (result = '0-1') THEN 1 ELSE 0 END)) BlackWins
   , COUNT(*) TotalDecisiveGames
   FROM
     test1.data_source_12
   WHERE (result IN ('1-0', '0-1'))
) 
, WinRates AS (
   SELECT
     WhiteWins
   , BlackWins
   , TotalDecisiveGames
   , ((WhiteWins / CAST(TotalDecisiveGames AS DOUBLE)) * 100) WhiteWinRate
   , ((BlackWins / CAST(TotalDecisiveGames AS DOUBLE)) * 100) BlackWinRate
   FROM
     GameOutcomes
) 
SELECT
  WhiteWins
, BlackWins
, WhiteWinRate
, BlackWinRate
FROM
  WinRates

SELECT
  'White' PlayerColor
, WhiteWinRate WinRate
FROM
  "whiteblacktotalwinrate"
UNION ALL SELECT
  'Black' PlayerColor
, BlackWinRate WinRate
FROM
  "whiteblacktotalwinrate"