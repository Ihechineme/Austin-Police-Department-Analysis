/* CREATE TABLE apd_calls (
    incident_number int,
    incident_type text,
    mental_health_flag text,
    priority_level text,
    response_datetime text,
    response_day_of_week text,
    response_hour int,
    first_unit_arrived_date text,
    call_closed_date text,
    sector text,
    initial_problem_desc text,
    initial_problem_category text,
    final_problem_desc text,
    final_problem_category text,
    no_of_units_arrived int,
    unit_time_on_scene int,
    call_disposition_desc text,
    report_written_flag text,
    response_time int,
    officer_injured_killed_count int,
	subject_injured_killed_count  int,
	other_injured_killed_count  int,
    geo_id bigint,
    census_block_group bigint,
    council_district int
); --

SELECT *
FROM apd_calls;

SHOW COLUMNS FROM apd_calls;

SHOW VARIABLES LIKE "LOCAL_INFILE";

LOAD DATA LOCAL INFILE "C:\\Users\\User\\Desktop\\SQL Files\\apd_calls.csv"
INTO TABLE apd_calls2
FIELDS TERMINATED BY ','
IGNORE 1 ROWS; 

This code was used to create the table for apd_calls2 and also import data into the table. Query was accidentally deleted so I am saving this 
here. */ 

-- checking for duplicates

SELECT *
FROM apd_calls2
WHERE incident_number IN (
    SELECT incident_number
    FROM apd_calls2
    GROUP BY incident_number
    HAVING COUNT(*) > 1
)
LIMIT 0, 1000;

 SELECT incident_number, COUNT(*)
 FROM apd_calls2
 GROUP BY 1
 HAVING COUNT(*) > 1
 ORDER BY 2 desc
 LIMIT 10;

SELECT *
FROM apd_calls2
WHERE incident_number = 240191419;

DELETE FROM apd_calls2
WHERE incident_number = 0;

SET @@global.wait_timeout=600; -- sets to 300 seconds
SET @@global.interactive_timeout=600;

SET @@global.connect_timeout=600;

START TRANSACTION;

ALTER TABLE apd_calls2
ADD COLUMN id INT AUTO_INCREMENT PRIMARY KEY;

rollback;

ALTER TABLE apd_calls2
DROP COLUMN id;

 SELECT incident_number, initial_prob_category, final_prob_category, geo_id,  COUNT(*)
 FROM apd_calls2
 GROUP BY 1, 2, 3, 4
 HAVING COUNT(*) > 1
 ORDER BY 2 desc
 LIMIT 10;
 
CREATE TABLE apd_calls_staging AS SELECT * FROM apd_calls2;

SELECT *
FROM apd_calls_staging
LIMIT 20;

 SELECT incident_number, COUNT(*)
 FROM apd_calls_staging
 GROUP BY 1
 HAVING COUNT(*) > 1
 ORDER BY 2 desc
 LIMIT 10;

SELECT *
FROM apd_calls2
WHERE incident_number = 221781641;

/*CREATE TABLE apd_calls_staging (
    incident_number int,
    incident_type text,
    mental_health_flag text,
    priority_level text,
    response_datetime text,
    response_day_of_week text,
    response_hour int,
    first_unit_arrived_date text,
    call_closed_date text,
    sector text,
    initial_problem_desc text,
    initial_problem_category text,
    final_problem_desc text,
    final_problem_category text,
    no_of_units_arrived int,
    unit_time_on_scene int,
    call_disposition_desc text,
    report_written_flag text,
    response_time int,
    officer_injured_killed_count int,
	subject_injured_killed_count  int,
	other_injured_killed_count  int,
    geo_id bigint,
    census_block_group bigint,
    council_district int,
    row_num int
);

INSERT INTO apd_calls_staging (
incident_number,
    incident_type,
    mental_health_flag,
    priority_level,
    response_datetime,
    response_day_of_week,
    response_hour,
    first_unit_arrived_date,
    call_closed_date,
    sector,
    initial_problem_desc,
    initial_problem_category,
    final_problem_desc,
    final_problem_category,
    no_of_units_arrived,
    unit_time_on_scene,
    call_disposition_desc,
    report_written_flag,
    response_time,
    officer_injured_killed_count,
    subject_injured_killed_count,
    other_injured_killed_count,
    geo_id,
    census_block_group,
    council_district,
    row_num
)
SELECT 
    incident_number,
    incident_type,
    mental_health_flag,
    priority_level,
    response_datetime,
    response_day_of_week,
    response_hour,
    first_unit_arrived_date,
    call_closed_date,
    sector,
    initial_problem_desc,
    initial_prob_category,
    final_prob_desc,
    final_prob_category,
    no_of_units_arrived,
    unit_time_on_scene,
    call_disposition_desc,
    report_written_flag,
    response_time,
    officer_injured_killed_count,
    subject_injured_killed_count,
    other_injured_killed_count,
    geo_id,
    census_block_group,
    council_district,
    ROW_NUMBER() OVER (
        PARTITION BY incident_number, incident_type, mental_health_flag, priority_level,
                     response_datetime, response_day_of_week, response_hour,
                     first_unit_arrived_date, call_closed_date, sector,
                     initial_problem_desc, initial_prob_category, final_prob_desc,
                     final_prob_category, no_of_units_arrived, unit_time_on_scene,
                     call_disposition_desc, report_written_flag, response_time,
                     officer_injured_killed_count, subject_injured_killed_count,
                     other_injured_killed_count, geo_id, census_block_group,
                     council_district 
                     ) AS row_num
                     FROM apd_calls2;*/
                     
                     
CREATE TABLE apd_calls_staging AS SELECT * FROM apd_calls2;      

SELECT *
FROM apd_calls_staging
LIMIT 35;
    
CREATE TABLE temp_table AS
SELECT DISTINCT * 
FROM apd_calls_staging;

ALTER TABLE temp_table RENAME TO apd_calls_test;

-- cross-checking that duplicates were removed
 WITH duplicate_cte AS (
    SELECT *, 
           ROW_NUMBER() OVER (
               PARTITION BY incident_number, incident_type, mental_health_flag, priority_level,
                            response_datetime, response_day_of_week, response_hour,
                            first_unit_arrived_date, call_closed_date, sector,
                            initial_problem_desc, initial_prob_category, final_prob_desc,
                            final_prob_category, no_of_units_arrived, unit_time_on_scene,
                            call_disposition_desc, report_written_flag, response_time,
                            officer_injured_killed_count, subject_injured_killed_count,
                            other_injured_killed_count, geo_id, census_block_group,
                            council_district
           ) AS row_num
	FROM apd_calls_test)
    SELECT *
    FROM duplicate_cte
    WHERE row_num >= 2; -- no rows were returned. Duplicates removed
    
SELECT COUNT(*)
FROM apd_calls_test; -- checking number of rows to be certain
    
SELECT COUNT(*)
FROM apd_calls2; -- rows reduced from 1.9 million to about 952k rows. Duplicates removed.

-- Removing null values

SELECT *
FROM apd_calls_test
LIMIT 20;

SELECT *
FROM apd_calls_test
WHERE call_disposition_desc IS NULL
   OR report_written_flag IS NULL
   OR response_time IS NULL
   OR officer_injured_killed_count IS NULL
   OR subject_injured_killed_count IS NULL
   OR other_injured_killed_count IS NULL
   OR geo_id IS NULL
   OR census_block_group IS NULL
   OR council_district IS NULL; -- checked each column by batch to reduce query time

DELETE FROM apd_calls_test
WHERE call_disposition_desc IS NULL
   OR report_written_flag IS NULL
   OR response_time IS NULL
   OR officer_injured_killed_count IS NULL
   OR subject_injured_killed_count IS NULL
   OR other_injured_killed_count IS NULL
   OR geo_id IS NULL
   OR census_block_group IS NULL
   OR council_district IS NULL;

SELECT *
FROM apd_calls_test
WHERE call_disposition_desc = ''
OR report_written_flag = ''
   OR response_time = ''
   OR geo_id = ''
   OR census_block_group = '';
   
-- Correcting wrong datatypes
SHOW COLUMNS FROM apd_calls_test;

ALTER TABLE apd_calls_test ADD COLUMN response_date DATETIME;

UPDATE apd_calls_test
SET response_date = STR_TO_DATE(response_datetime, '%m/%d/%Y %h:%i:%s %p');

UPDATE apd_calls_test
SET response_datetime = STR_TO_DATE(response_datetime, '%m/%d/%Y %h:%i:%s %p');

UPDATE apd_calls_test
SET first_unit_arrived_date = STR_TO_DATE(first_unit_arrived_date, '%m/%d/%Y %h:%i:%s %p');

UPDATE apd_calls_test
SET call_closed_date = STR_TO_DATE(call_closed_date, '%m/%d/%Y %h:%i:%s %p');

ALTER TABLE apd_calls_test
DROP COLUMN response_date;

-- Data Analysis

SELECT sector, council_district, AVG(response_time)
FROM apd_calls_test
GROUP BY 1, 2
ORDER BY 3 DESC; 
/* This query was used to find the sectors with the highest response time. Top 3 sectors with the highest response time was
Charlie(district 6), Frank(district 8) and Henry(disrict 2) with an avearge response time of 5703, 3318 and 3191 respectively. I'll run another query to determine which districts have the 
highest avg response time. */

SELECT council_district, AVG(response_time)
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5; 
-- Top five districts with the highest average response times are 1, 4, 2, 6, 7 with the highest avg time being 2609.95 secs. 

-- Analysing factors affecting response time by day of the week, time of day and priority level

-- day of the week
SELECT response_day_of_week, AVG(response_time)
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC
LIMIT 3; -- days of the week with the highest avg response time; Saturday, Sunday and Monday

-- hour of the day
SELECT response_hour, AVG(response_time)
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC
LIMIT 3;  -- top 3 avg response time by hour of the day; 13:00, 14:00 and 11:00

-- I'll try to compare the number of incidents occuring at these times with the average response time

SELECT response_hour, 
    COUNT(*) AS call_count,               -- Total number of calls for each hour
    AVG(response_time) AS avg_response_time -- Average response time for each hour
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC
LIMIT 10;
/* From this query, it seems like the lesser the number of calls, the higher the response time. For instance, 17:00 has the highest number of 911 calls 
but the avg response time is about 15 min less than 13:00 which has the highest resp. time on the list. I'll try to pull the min and response time 
as well as the min and max call count */

SELECT 
    MIN(call_count) AS min_call_count, 
    MIN(avg_response_time) AS min_avg_response_time, 
    MAX(call_count) AS max_call_count, 
    MAX(avg_response_time) AS max_avg_response_time
FROM (
    SELECT 
        response_hour, 
        COUNT(*) AS call_count,               
        AVG(response_time) AS avg_response_time 
    FROM 
        apd_calls_test
    GROUP BY 
        response_hour
) AS hourly_stats;

SELECT response_hour, 
    COUNT(*) AS call_count,              
    AVG(response_time) AS avg_response_time 
FROM apd_calls_test
GROUP BY 1
ORDER BY 3 
LIMIT 1;

/* after cross-refrencing the two results, it is observed from the result of the queries that for hours where that had higher call volume, 
the avg response time was lesser. It could be that more units are deployed during these busy hours. This will be checked with a query 

I'll repeat the same queries above, this time for day of the week*/

SELECT response_day_of_week, 
    COUNT(*) AS call_count,               -- Total number of calls for each day
    AVG(response_time) AS avg_response_time -- Average response time for each day
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC;  /* Friday has the highest volume of calls and the least response time which is a good thing. However, sat. sun. and mon. which have the highest call volume after friday 
have the highest response times. There's probably not enough people on ground to attend to these calls as soon as possible on these days. No of units deployed on these days will be checked 
to ascertain if the analysis is correct.*/

-- response time by priority level

SELECT priority_level, AVG(response_time)
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC; -- incidents identified as high priority have the lowest avg response time. 

-- Checking efficiency of resource allocation. Check officers dispatched by day, hour, district, priority level

SELECT *
FROM apd_calls_test
WHERE no_of_units_arrived = 101
OR no_of_units_arrived = 92
LIMIT 20;

-- UNITS DEPLOYED BY DAY OF THE WEEK 

SELECT response_day_of_week, MAX(no_of_units_arrived), AVG(no_of_units_arrived)
FROM apd_calls_test
GROUP BY 1
ORDER BY 2; /*average number of units arrived is approximately 2 for every day of the week. However, it is advised that more officers be deployed 
			on the weekends where the volume of calls are higher. We will also examine how many units where deployed for priority incidents. */

-- UNITS DEPLOYED BY PRIORITY LEVEL

SELECT priority_level, MAX(no_of_units_arrived), AVG(no_of_units_arrived)
FROM apd_calls_test
GROUP BY 1
ORDER BY 3 DESC; 
-- As expected, incidents bearing a higher priority level had a higher average number of units deployed.

SELECT priority_level, response_day_of_week, MAX(no_of_units_arrived), AVG(no_of_units_arrived)
FROM apd_calls_test
GROUP BY 1, 2
ORDER BY 4 DESC;

-- UNITS  DEPLOYED BY HOUR

SELECT response_hour, MAX(no_of_units_arrived), AVG(no_of_units_arrived)
FROM apd_calls_test
GROUP BY 1
ORDER BY 3 DESC;

-- BY INCIDENT TYPE

SELECT incident_type, MAX(no_of_units_arrived), AVG(no_of_units_arrived)
FROM apd_calls_test
GROUP BY 1
ORDER BY 3 DESC; -- no officer initiated incidents

-- BY DISTRICT AND SECTOR

SELECT council_district, sector, MAX(no_of_units_arrived), AVG(no_of_units_arrived)
FROM apd_calls_test
GROUP BY 1, 2
ORDER BY 4 DESC;

-- Monitoring Priority Level

-- calculating fatality 
SELECT SUM(officer_injured_killed_count) AS officer_fatality,
		SUM(subject_injured_killed_count) AS subject_fatality,
        SUM(other_injured_killed_count) AS other_fatality
FROM apd_calls_test;

-- 1 officer fatality, 3 subject fatality and 0 other fatality
SELECT priority_level,
		SUM(officer_injured_killed_count) AS officer_fatality,
		SUM(subject_injured_killed_count) AS subject_fatality,
        SUM(other_injured_killed_count) AS other_fatality
FROM apd_calls_test
GROUP BY 1; -- Fatality was spread out among priority levels. No priority level had a significantly higher number of fatalities than the other.

SELECT priority_level, sector,
	COUNT(priority_level) AS priority_level_count
FROM apd_calls_test
GROUP BY 1, 2
ORDER BY 3;

SELECT 
    t3.priority_level, 
    t3.sector, 
    t3.priority_count
FROM (
    SELECT 
        priority_level, 
        MAX(priority_count) AS max_priority_count
    FROM (
        SELECT 
            priority_level, 
            sector, 
            COUNT(priority_level) AS priority_count
        FROM 
            apd_calls_test
        GROUP BY 
            priority_level, sector
    ) AS t1
    GROUP BY 
        priority_level
) AS t2
JOIN (
    SELECT 
        priority_level, 
        sector, 
        COUNT(priority_level) AS priority_count
    FROM 
        apd_calls_test
    GROUP BY 
        priority_level, sector
) AS t3
ON 
    t3.priority_level = t2.priority_level 
    AND t3.priority_count = t2.max_priority_count;
    
    SELECT 
    t3.priority_level, 
    t3.sector, 
    t3.council_district, 
    t3.priority_count
FROM (
    SELECT 
        priority_level, 
        MAX(priority_count) AS max_priority_count
    FROM (
        SELECT 
            priority_level, 
            sector, 
            council_district, 
            COUNT(priority_level) AS priority_count
        FROM 
            apd_calls_test
        GROUP BY 
            priority_level, sector, council_district
    ) AS t1
    GROUP BY 
        priority_level
) AS t2
JOIN (
    SELECT 
        priority_level, 
        sector, 
        council_district, 
        COUNT(priority_level) AS priority_count
    FROM 
        apd_calls_test
    GROUP BY 
        priority_level, sector, council_district
) AS t3
ON 
    t3.priority_level = t2.priority_level 
    AND t3.priority_count = t2.max_priority_count;  /* The aim of the query was to find the sector and council district with the highest count of each priority
														level. Sector Edward in district 4 had the max count for each priority level. */
                                                        
-- Which priority level has the maximum count on each table?
SELECT priority_level, COUNT(*)
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC; -- priority 2 had the most number of calls 

-- Location and incident analysis

-- finding the highest number of calls by location - sector and district
SELECT sector, council_district, COUNT(*)
FROM apd_calls_test 
GROUP BY 1, 2
ORDER BY 3 DESC
LIMIT 5; 

-- Top 5 incident hotspots: Edward (4), Frank(2), Henry(3), Charlie(1), Adam(6) 

SELECT *
FROM apd_calls_test
LIMIT 5;

SELECT 
    final_prob_category,
    sector,
    council_district,
    AVG(no_of_units_arrived) AS avg_units_dispatched,
    AVG(no_of_units_arrived) - 
        AVG(AVG(no_of_units_arrived)) OVER () AS units_discrepancy_from_overall_avg
FROM 
    apd_calls_test
GROUP BY 
  1, 2, 3
ORDER BY 5 DESC
LIMIT 10;
 /* This query aims to the average units dispatched for a sector and district combination against the overall average. It checks how much the individual averages
 vary from the overall average. it can be observed from the result that Homicide cases have more units on ground which is not abnormal. Let's run the reverse
 of this query to see which type of cases get the least no of units. */
 
 SELECT 
    final_prob_category,
    sector,
    council_district,
    AVG(no_of_units_arrived) AS avg_units_dispatched,
    AVG(no_of_units_arrived) - 
        AVG(AVG(no_of_units_arrived)) OVER () AS units_discrepancy_from_overall_avg
FROM 
    apd_calls_test
GROUP BY 
  1, 2, 3
ORDER BY 5
LIMIT 10; 
/* The following incident categories have a negative discrepancey. Viol PO/Bond, Sex Crimes, Animal Related, Drugs, Disturbance. This means that these
crimes gets a lower no of units than the overall average. */

-- Average no of units dispatched per sector

SELECT sector, council_district, AVG(no_of_units_arrived) avg_unit_dispatched
FROM apd_calls_test
GROUP BY 1,2
ORDER BY 3 DESC
LIMIT 5;

-- Average no of units dispatched per incident category

SELECT initial_prob_category, AVG(no_of_units_arrived) avg_unit_dispatched
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;

-- Average no of units dispatched for mental health related incidents
SELECT mental_health_flag, AVG(no_of_units_arrived) avg_unit_dispatched
FROM apd_calls_test
GROUP BY 1;

-- Most common incident types
SELECT final_prob_category, COUNT(*) count_of_incident_category 
FROM apd_calls_test
GROUP BY 1
ORDER BY 2 DESC; -- Most common incident category is Disturbance. let's check how many of these are mental health related

SELECT final_prob_category, COUNT(*) count_of_incident_category,
COUNT(CASE WHEN mental_health_flag = 'Mental Health Incident' THEN 1 END) / COUNT(*) * 100 AS mental_health_percentage
FROM apd_calls_test
GROUP BY 1
ORDER BY 3 DESC; -- Welfare check has the highest mental health related incidents.

-- Average response times for mental health related incidents
SELECT mental_health_flag, AVG(response_time) avg_response_time
FROM apd_calls_test
GROUP BY 1; -- there isn't a huge disparity in response times for this category.

-- checking trends in incident categories - by day, location, sector and district


SELECT final_prob_category, response_day_of_week, response_hour, COUNT(*) AS incident_count
FROM apd_calls_test
GROUP BY 1, 2, 3
ORDER BY 4 DESC;


                             
    