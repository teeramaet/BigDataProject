WITH
  PlayerMonthlyElo AS (
   SELECT
     player
   , DATE_TRUNC('month', utcdate) month
   , FIRST_VALUE(elo) OVER (PARTITION BY player, DATE_TRUNC('month', utcdate) ORDER BY utcdate ASC) first_elo
   , LAST_VALUE(elo) OVER (PARTITION BY player, DATE_TRUNC('month', utcdate) ORDER BY utcdate ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_elo
   FROM
     (
      SELECT
        white player
      , whiteelo elo
      , utcdate
      FROM
        test1.data_source_12
UNION ALL       SELECT
        black player
      , blackelo elo
      , utcdate
      FROM
        test1.data_source_12
   )  AllPlayerElos
) 
, PlayerEloDiff AS (
   SELECT
     player
   , month
   , (last_elo - first_elo) elo_diff
   FROM
     PlayerMonthlyElo
) 
, PlayerGameCounts AS (
   SELECT
     player
   , COUNT(*) games_played
   FROM
     (
      SELECT white player
      FROM
        test1.data_source_12
UNION ALL       SELECT black player
      FROM
        test1.data_source_12
   )  AllGames
   GROUP BY player
) 
, RankedPlayers AS (
   SELECT
     p.player
   , p.games_played
   , AVG(e.elo_diff) OVER (PARTITION BY p.player) avg_elo_diff
   , NTILE(5) OVER (ORDER BY p.games_played ASC) bin
   FROM
     (PlayerGameCounts p
   INNER JOIN PlayerEloDiff e ON (p.player = e.player))
) 
, BinStats AS (
   SELECT
     bin
   , COUNT(*) num_players
   , MIN(games_played) min_games_played
   , MAX(games_played) max_games_played
   , AVG(avg_elo_diff) avg_elo_diff_per_bin
   FROM
     RankedPlayers
   GROUP BY bin
) 
SELECT
  bin
, num_players
, CONCAT(CAST(min_games_played AS VARCHAR), ' - ', CAST(max_games_played AS VARCHAR)) games_played_range
, avg_elo_diff_per_bin
FROM
  BinStats
ORDER BY bin ASC