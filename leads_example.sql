--Task: we have leads weekly aggregated, we need to calculate leads monthly, so we split the values into days and then assign them monthly


--getting the raw data weekly aggregated
DROP TABLE IF EXISTS public.map_year_agg_raw;
CREATE TABLE public.map_year_agg_raw AS
SELECT
region_name
, CASE
  WHEN object_type IN ('cottage') THEN 'cottage'
  WHEN object_type IN ('store') THEN 'commerce'
  WHEN deal_type IN ('sale') AND object_type IN ('urban', 'newbuildings') THEN 'sale residential'
  WHEN deal_type IN ('rent') AND object_type IN ('urban') THEN 'rent residential'
  ELSE 'dummy'
  END AS rubric
, CASE WHEN source in ('source1', 'source2') THEN source ELSE 'other' END AS player

, CASE
   WHEN is_region_capital = 1 THEN 'center'
   ELSE 'rural'
   END as centr 
, ptn_dadd
, sum(cnt) as leads

FROM
public.map_measurement_result_new_split
where
object_type <> 'garage'
and
ptn_dadd >= date'2022-01-01'
GROUP BY
region_name
, CASE
  WHEN object_type IN ('cottage') THEN 'cottage'
  WHEN object_type IN ('land', 'store') THEN 'commerce'
  WHEN deal_type IN ('sale') AND object_type IN ('urban', 'newbuildings') THEN 'sale residential'
  WHEN deal_type IN ('rent') AND object_type IN ('urban') THEN 'rent residential'
  ELSE 'dummy'
  END
, CASE WHEN source in ('source1', 'source2') THEN source ELSE 'other' END
, CASE
   WHEN is_region_capital = 1 THEN 'center'
   ELSE 'rural'
   END
;


--getting the totals for all region players
DROP TABLE IF EXISTS public.map_year_agg_total;
CREATE TABLE public.map_year_agg_total AS
select
region_name
, rubric
, player
, centr
, ptn_dadd
, leads
, sum(leads) OVER(PARTITION BY region_name, rubric, centr, ptn_dadd) as total
from
public.map_year_agg_raw
group by
region_name
, rubric
, player
, centr
, ptn_dadd
, leads
;

--setting the days of the week
DROP TABLE IF EXISTS public.map_year_agg_days;
CREATE TABLE public.map_year_agg_days AS
with a as (
select
region_name
, rubric
, player
, centr
, ptn_dadd
, leads
, total
, week_start
, week_end
FROM
public.map_year_agg_total att
JOIN
public_stg.calendar cal
ON
att.ptn_dadd = cal.dat
)

select distinct
a.*
, sc.dat
from
a
join
public_stg.calendar sc
on 
a.week_end = sc.week_end
;


--average value for each day
DROP TABLE IF EXISTS public.map_year_agg_leads_avg;
CREATE TABLE public.map_year_agg_leads_avg AS
SELECT
*
, leads / 7.00 as leads_avg
, total / 7.00 as total_avg
, month(dat) as mon
from
public.map_year_agg_days
;




--assigning value for the given month

select distinct
region_name
, rubric
, player
, centr
, sum(leads_avg) OVER(PARTITION BY region_name, rubric, player, centr, mon)as mon_leads
, sum(total_avg) OVER(PARTITION BY region_name, rubric, player, centr, mon)as total_leads
, mon
from
public.map_year_agg_leads_avg
order by
mon
, region_name
, rubric
, centr
;



