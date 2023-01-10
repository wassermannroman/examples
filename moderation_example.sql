--Task: creating the datamart with user's identification types and optimizing it for Tableau


--getting the active announcements
DROP TABLE IF EXISTS public.moder_ident_kpi_daily_ac;
CREATE TABLE public.moder_ident_kpi_daily_ac AS
  WITH temp AS ( --taking active announcements from yesterday
    SELECT
      id
      , userid3
      , geo_address_id
      , category
      , ptn_dadd
  FROM
    announcements
  WHERE
    ptn_dadd = current_date - INTERVAL'1'DAY
  AND
    COALESCE(wasactive, 1) = 1
  AND
    COALESCE(isinhiddenbase, false) = false
  AND
    (platform_type <> 'qaAutotests' OR platform_type IS NULL)
)

SELECT --keeping only certain categories
  ac.id
  , ac.userid3
  , l.region_name
  , ac.ptn_dadd as dadd 
FROM
  temp ac
JOIN
  locations l --join locations table for getting the region name
ON
  l.id = ac.geo_address_id
JOIN
  categories bc 
ON
  ac.category = bc.category
WHERE 
  bc.rubric_objectcategory IN ('rural', 'urban')
AND 
  bc.rubric_name NOT IN ('garage', 'newbuilding')
;

--finding the main account and publishing regions
DROP TABLE IF EXISTS public.moder_ident_kpi_publishers;
CREATE TABLE public.moder_ident_kpi_publishers AS
SELECT 
    ac.userid3
    , COALESCE(c.userid2, c.userid1) main_acc
    , array_join(array_agg(distinct ac.region_name),', ') region_name
    , ac.dadd
FROM 
  public.moder_ident_kpi_daily_ac ac
JOIN
  clients AS c
ON
  ac.userid3 = c.userid1
GROUP BY 
  ac.userid3
  , COALESCE(c.userid2, c.userid1)
  , ac.dadd
;

DROP TABLE IF EXISTS public.moder_ident_kpi_publisher_daily_ac;
CREATE TABLE public.moder_ident_kpi_publisher_daily_ac AS
SELECT
  p.userid3
  , p.main_acc
  , p.region_name
  , COALESCE(lm.region_name, 'unknown') AS region 
  , lm.focus_region
  , c.tarif as ctarif
  , p.dadd
FROM
  public.moder_ident_kpi_publishers AS p
JOIN 
  clients AS c
ON
  p.main_acc = c.userid1 
LEFT JOIN
  locations AS lm
ON
  COALESCE (c.mainannouncementsregionid, c.orf_region) = lm.id
WHERE
  c.tarif NOT IN ('builder')
;

--adding user segments
DROP TABLE IF EXISTS public.moder_ident_kpi_segments_daily_ac;
CREATE TABLE public.moder_ident_kpi_segments_daily_ac AS
SELECT 
  us.segment
  , p.dadd
  , p.main_acc
  , p.region
  , p.region_name
FROM
  public.moder_ident_kpi_publisher_daily_ac p 
JOIN  
  segments us
ON
  p.main_acc = us.userid
WHERE
  us.period = current_date - INTERVAL'1'DAY
AND
  us.method_id = 1
AND
  us.segment IN ('a','b','c') 
GROUP BY
 us.segment
  , p.dadd
  , p.main_acc
  , p.region
  , p.region_name
;
--transitional table for identification types
DROP TABLE IF EXISTS public.daily_moder_ident_kpi_ac;
CREATE TABLE public.daily_moder_ident_kpi_ac AS
SELECT
  s.main_acc 
  , s.segment
  , s.region_name
  , s.region
  , array_join(i.identificationtype,', ') as identificationtype
  , s.dadd
FROM 
  public.moder_ident_kpi_segments_daily_ac s
LEFT JOIN
  ident i 
ON  
  i.userid = s.main_acc  
AND 
  i.dadd = s.dadd
GROUP BY
   s.main_acc
  , s.segment
  , s.region_name
  , s.region
  , s.dadd
  , array_join(i.identificationtype,', ')
;
--transitional table for Tableau short source
DROP TABLE IF EXISTS public.daily_moder_ident_kpi_type_ac;
CREATE TABLE public.daily_moder_ident_kpi_type_ac AS
SELECT
   main_acc 
  , segment
  , region_name
  , region
  , identificationtype
  , dadd
  , CASE
     WHEN
       regexp_like(identificationtype, 'type1') 
     OR
       regexp_like(identificationtype, 'type2')
     OR
       regexp_like(identificationtype, 'type3')
     OR
       regexp_like(identificationtype, 'type4')
     THEN
       1
     ELSE
       0
    END AS is_identified
FROM
  public.daily_moder_ident_kpi_ac
;

--Tableau short datamart
set session hive.insert_existing_partitions_behavior = 'overwrite';
INSERT INTO reporter.daily_moder_ident_kpi_short
SELECT 
  CASE
    WHEN
      segment IN ('a','b')
    THEN
      'segment1'
    WHEN
      segment = 'c'
    THEN
      'segment2'
    ELSE
      'other' 
    END AS segment
  , is_identified
  , COUNT(DISTINCT main_acc) AS cnt_users
  , dadd
FROM 
  public.daily_moder_ident_kpi_type_ac s
GROUP BY  
  is_identified
  , dadd
    , CASE
      WHEN
        segment IN ('a','b')
      THEN
        'segment1'
      WHEN
        segment = 'c'
      THEN
        'segment2'
      ELSE
        'other' 
      END
;

--transitional table for long Tableau datamart
set session hive.insert_existing_partitions_behavior = 'overwrite'; 
INSERT INTO public.daily_moder_ident_cnt
SELECT
  s.segment
  , s.region_name
  , s.region
  , i.identificationtype
  , COUNT(DISTINCT s.main_acc) as cnt
  , CASE 
      WHEN
        any_match(i.identificationtype, x -> x IN ('type1', 'type2', 'type3', 'type4'))
      THEN
        1
      ELSE
        0
      END is_identified
   , s.dadd
FROM 
  public.moder_ident_kpi_segments_daily_ac s
LEFT JOIN
  ident i
ON  
  i.userid = s.main_acc  
AND 
  i.dadd = s.dadd
GROUP BY
  s.segment
  , s.region_name
  , s.region
  , s.dadd
  , i.identificationtype
  , CASE 
      WHEN
        any_match(i.identificationtype, x -> x IN ('type1', 'type2', 'type3', 'type4'))
      THEN
        1
      ELSE
        0
      END
;
--creating table with row numbers for Tableau's datamart optimization 
DROP TABLE IF EXISTS public.daily_moder_ident_number;
CREATE TABLE public.daily_moder_ident_number AS
SELECT
  row_number() OVER() as rn --the unique number is a key for joining the table below
  , segment
  , region_name
  , region
  , identificationtype
  , cnt
  , is_identified
  , dadd
FROM
  public.daily_moder_ident_cnt
; 
--unnest the identification types
DROP TABLE IF EXISTS public.daily_moder_ident_kpi_unfold;
CREATE TABLE public.daily_moder_ident_kpi_unfold AS
SELECT
  rn
  , t.ident_type
FROM
  public.daily_moder_ident_number
LEFT JOIN 
  UNNEST (identificationtype) t (ident_type)
ON true
;

--Then in Tableau we create the relation on 'rn' field with table above. It's needed for reducing the rows count and higher Tableau performance 
DROP TABLE IF EXISTS public.daily_moder_ident_cnt_res;
CREATE TABLE public.daily_moder_ident_cnt_res AS
SELECT
  rn
  , segment
  , region_name
  , region
  , cnt
  , is_identified
  , dadd
FROM
  public.daily_moder_ident_number
;

