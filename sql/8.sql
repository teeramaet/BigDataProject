SELECT
  Event
, Termination
, COUNT(*) Count
FROM
  test1.data_source_12
GROUP BY Event, Termination