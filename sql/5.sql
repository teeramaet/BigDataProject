WITH
  Rating_Opening_Frequencies AS (
   SELECT
     (CASE WHEN (elo >= 2700) THEN '2700+ (Super Grandmasters)' WHEN (elo BETWEEN 2500 AND 2699) THEN '2500-2699 (Grandmasters)' WHEN (elo BETWEEN 2400 AND 2499) THEN '2400-2499 (IMs)' WHEN (elo BETWEEN 2300 AND 2399) THEN '2300-2399 (FMs)' WHEN (elo BETWEEN 2200 AND 2299) THEN '2200-2299 (NMs)' WHEN (elo BETWEEN 2000 AND 2199) THEN '2000-2199 (CMs)' WHEN (elo BETWEEN 1800 AND 1999) THEN '1800-1999 (Class A)' WHEN (elo BETWEEN 1600 AND 1799) THEN '1600-1799 (Class B)' WHEN (elo BETWEEN 1400 AND 1599) THEN '1400-1599 (Class C)' WHEN (elo BETWEEN 1200 AND 1399) THEN '1200-1399 (Class D)' WHEN (elo BETWEEN 1000 AND 1199) THEN '1000-1199 (Class E)' ELSE '0-999 (Novices)' END) Rating_Category
   , opening
   , COUNT(*) Opening_Count
   FROM
     (
      SELECT
        whiteelo elo
      , opening
      FROM
        test1.data_source_12
UNION ALL       SELECT
        blackelo elo
      , opening
      FROM
        test1.data_source_12
   )  player_openings
   WHERE ((elo IS NOT NULL) AND (opening IS NOT NULL))
   GROUP BY 1, opening
) 
, Ranked_Openings AS (
   SELECT
     Rating_Category
   , opening
   , Opening_Count
   , RANK() OVER (PARTITION BY Rating_Category ORDER BY Opening_Count DESC) rank
   FROM
     Rating_Opening_Frequencies
) 
SELECT
  Rating_Category
, opening
, Opening_Count
FROM
  Ranked_Openings
WHERE (rank = 1)
ORDER BY Rating_Category ASC