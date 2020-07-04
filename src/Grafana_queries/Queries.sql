
/*--------------------Query for live values---------------*/

SELECT
  "time" AS "time",
  value, (select avg(value) from phoenix b where b.week = date_part('week', a.time) and date_part('year', b.time) = 2019 and b.parameter = 'pm10') as weekly_avg
FROM phoenix a
WHERE
  $__timeFilter("time") and a.parameter = 'pm10'
ORDER BY 1

/*--------------------Query for annual averages---------------*/

SELECT
  "time" AS "time",
  value, (select avg(value) from usa2 b where b.zip = 85006 and b.parameter = 'pm10' and b.week = date_part('week', a.time) and date_part('year', b.time) = 2019) as weekly_avg
FROM usa2 a
WHERE
  a.zip = 85006 and a.parameter = 'pm10' and $__timeFilter("time")
ORDER BY 1

