create database maude;

use database maude;

create schema raw;

use schema raw;

CREATE or replace TABLE maude_events (
    report_number STRING,
    event_type STRING,
    manufacturer STRING,
    product_code STRING,
    brand_name STRING,
    device_problem STRING,
    patient_problem STRING,
    pma_pmn_number STRING,
    event_text STRING,

    eventdate_date NUMBER,
    eventdate_month NUMBER,
    eventdate_quarter NUMBER,
    eventdate_year NUMBER,

    date_received_date NUMBER,
    date_received_month NUMBER,
    date_received_quarter NUMBER,
    date_received_year NUMBER,

    severe NUMBER
);

show stages;

LIST @~;


select * from maude_events;

create stage maude_stage;

--upload data in stage through UI in maude_stage - raw schema
list @maude_stage;  -- to see whats inside the stage

-- load into table
COPY INTO maude_events
FROM @maude_stage/maude.csv
FILE_FORMAT = (
    TYPE = CSV
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
);


SELECT * FROM maude_events;  -- Bronze layer -  data as it is

--remove @maude_stage/Data.csv; -- wrong file uploaded

-- Total rows in table
select count(*)  from maude_events;


-- Create Silver Schema

create Schema Silver;

CREATE OR REPLACE TABLE silver.maude_events_clean AS
SELECT
    report_number,
    UPPER(TRIM(manufacturer)) AS manufacturer,
    UPPER(TRIM(brand_name)) AS brand_name,
    product_code,
    event_type,
    PMA_PMN_NUMBER,
    device_problem,
    patient_problem,
     eventdate_date,
    eventdate_year,
    eventdate_month,
    eventdate_quarter,
    date_received_year,
    date_received_month,
    date_received_quarter,
    date_received_date,
    severe
FROM raw.maude_events;

create schema gold;

select * from silver.maude_events_clean;

 --SELECT CURRENT_ROLE(); check if its accountadmin
 --SHOW GRANTS ON SCHEMA raw;

--SHOW GRANTS ON TABLE raw.maude_events;

--SELECT CURRENT_DATABASE(), CURRENT_SCHEMA();

-- Role-Based Access Control(RBAC), if permisiion not given to users for a particular layer
-- they cant access it


----------------------------------------------------------------------------------------------------
-- GOLD LAYER AGGREGATIONS

-- How many total records are in the dataset?
select count(*) from silver.maude_events_clean;

-- How many unique manufacturers exist?
select count( distinct manufacturer) from silver.maude_events_clean;

-- How many distinct product codes are there?
select count (distinct product_code) from silver.maude_events_clean;

-- What are the top 10 manufacturers by total events?
select manufacturer, count(report_number) as total_events from silver.maude_events_clean
group by manufacturer
order by total_events desc
limit 10;

-- How many events occurred for each event type?

select event_type, count(report_number) as number_of_events
from silver.maude_events_clean
group by event_type;

----------------------------------------------------------------------------

-- For each manufacturer, how many severe events occurred?

select manufacturer, count(report_number) as counts
from silver.maude_events_clean
where severe = 1
group by manufacturer;

-- What is the severe event ratio per manufacturer?

select manufacturer, avg(severe) as ratio
from silver.maude_events_clean
group by manufacturer;

-- Which month had the highest number of events?

select eventdate_month, count(report_number) as monthly_count
from silver.maude_events_clean
group by eventdate_month
order by monthly_count desc limit 1;

-- What is the monthly trend of total events?
select eventdate_month, count(report_number) as monthly_count
from silver.maude_events_clean
group by eventdate_month
order by monthly_count desc;

-- How many events per quarter per year?
select eventdate_quarter, count(report_number) as quarter_count
from silver.maude_events_clean
group by eventdate_quarter
order by quarter_count desc;

----------------------------------------------------------------------------

-- Which manufacturers have more than 50 events?
select manufacturer, count(report_number) as counts
from silver.maude_events_clean
group by manufacturer
having count(report_number) > 50;

-- Which manufacturers have a severe rate greater than 20%?
select manufacturer, avg(severe) as ratio
from silver.maude_events_clean
group by manufacturer
having avg(severe) > 0.2;

-- What are the top 5 product codes per year?
select product_code, count(report_number) as counts
from silver.maude_events_clean
group by product_code
order by counts desc
limit 5;

-- Which event type is most common per month?
with cte1 as (select eventdate_month, event_type,  count(report_number) as counts
from silver.maude_events_clean
group by event_type, eventdate_month),
cte2 as 
(select eventdate_month, event_type, counts,
dense_rank() over (partition by eventdate_month order by counts desc) as rnk
from cte1)
select eventdate_month, event_type, counts from cte2 where rnk  = 1
order by eventdate_month;

-- Which manufacturer has the highest average severe rate across months?
select manufacturer, avg(severe) as ratio
from silver.maude_events_clean
group by manufacturer
order by ratio desc limit 1;

------------------------------------------------------------------------------
--Create a manufacturer performance summary table with:
--Total events
--Total severe events
--Severe percentage
--First event month
--Latest event month

select manufacturer, count(report_number) as total_events,
sum(severe) as total_severe_events,
avg(severe) * 100 asseveer_percentage,
min(eventdate_month) as first_event_month,
max(eventdate_month) as latest_event_month
from silver.maude_events_clean
group by manufacturer;

-- Identify manufacturers with increasing monthly trend.
with  cte1 as (select manufacturer, eventdate_month, count(report_number) as total_events
from silver.maude_events_clean
group by manufacturer, eventdate_month),
cte2 as (select manufacturer, eventdate_month, total_events, lag(total_events) over (partition by manufacturer order by eventdate_month) as prev_total_count
from cte1),
cte3 as (select manufacturer, eventdate_month, prev_total_count, total_events - prev_total_count as diff
from cte2
where prev_total_count  is not null
)
select manufacturer from cte3 
GROUP BY manufacturer
having min(diff) > 0;


-- Find the top 3 manufacturers each year
with cte1 as (select manufacturer, count(report_number) as counts
from silver.maude_events_clean
group by manufacturer),
cte2 as (select manufacturer, counts, dense_rank() over (order by counts desc) as rnk
from cte1
)
select manufacturer from cte2 where rnk <= 3;

-- Compare device problem frequency vs patient problem frequency
select DEVICE_PROBLEM, patient_problem, count(*) from silver.maude_events_clean
group by DEVICE_PROBLEM, patient_problem;

-- Which month historically has the highest severe events?
 select eventdate_month, count(report_number) as counts
 from silver.maude_events_clean
 where severe = 1
 group by eventdate_month
 order by counts desc;

-- Calculate rolling 3-month event counts.
with cte1 as (select eventdate_month, count(report_number) as counts
from silver.maude_events_clean group by eventdate_month)
select eventdate_month, counts, sum(counts) over (order by eventdate_month rows between 2 preceding and current row) as rolling_3_month
from cte1;

-- Find manufacturers whose severe rate is above overall average severe rate.
select distinct manufacturer, avg(severe) over (partition by manufacturer) as severity_rate, avg(severe) over () as overall_average
from silver.maude_events_clean
qualify severity_rate > overall_average;

-- Identify outlier manufacturers using event volume.

with cte1 as (select manufacturer, count(report_number) as counts
from silver.maude_events_clean
group by manufacturer
),
cte2 as (select manufacturer, counts, max(counts) over () as max_count, min(counts) over ()
as min_count from cte1),

cte3 as (select manufacturer, counts, ((1.5*max_count) + max_count) as upper_bound ,
(min_count - (1.5*min_count)) as lower_bound
from cte2)

select manufacturer, counts
from cte3
where counts  > upper_bound or counts < lower_bound;


-- Find manufacturers that had zero events in a January but had events after.
select manufacturer
from silver.maude_events_clean
group by manufacturer
having sum(case when eventdate_month = 1 then 1 else 0 end) = 0
and SUM(CASE WHEN eventdate_month > 1 THEN 1 ELSE 0 END) > 0;
