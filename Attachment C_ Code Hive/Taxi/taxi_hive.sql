--*******************************
-- 1. Table TaxiTripData
--*******************************
-- original outout data after MapReduce
create external table TaxiTripData (tpep_pickup_datetime string, trip_distance string, pickup_location string)
row format delimited fields terminated by ','
location '/user/pc3095/projectMR/output/';

--*******************************
-- 2. Table TaxiTripWeekly
--*******************************
create table TaxiTripWeekly as 
select a.day_of_week, COUNT(*) as cnt
FROM (select extract(dayofweek from tpep_pickup_datetime) as day_of_week from TaxiTripData) a
GROUP BY a.day_of_week
HAVING COUNT(*) > 1
ORDER BY a.day_of_week;

/*
the table looks like this
from 1 (Sunday) to 7 Saturday)
+-----------------------------+---------------------+
| taxitripweekly.day_of_week  | taxitripweekly.cnt  |
+-----------------------------+---------------------+
| 1                           | 14547883            |
| 2                           | 15885027            |
| 3                           | 17941870            |
| 4                           | 18653845            |
| 5                           | 19201616            |
| 6                           | 19095377            |
| 7                           | 17382787            |
+-----------------------------+---------------------+
*/

--*******************************
-- 3. Table TaxiTripMonthly
--*******************************
create table TaxiTripMonthly as 
select a.year, a.month, COUNT(*) as cnt
FROM (select year(tpep_pickup_datetime) as year, 
month(tpep_pickup_datetime) as month from TaxiTripData) a
GROUP BY a.year, a.month
HAVING COUNT(*) > 1
ORDER BY a.year, a.month;

/*
the table looks like this
+---------+----------+----------+
| a.year  | a.month  |   cnt    |
+---------+----------+----------+
| 2018    | 12       | 348      |
| 2019    | 1        | 7612486  |
| 2019    | 2        | 6969320  |
| 2019    | 3        | 7778589  |
| 2019    | 4        | 7382242  |
| 2019    | 5        | 7508944  |
| 2019    | 6        | 6877849  |
| 2019    | 7        | 6242424  |
| 2019    | 8        | 6003604  |
| 2019    | 9        | 6496152  |
| 2019    | 10       | 7143872  |
| 2019    | 11       | 6802869  |
| 2019    | 12       | 6823240  |
| 2020    | 1        | 6334785  |
| 2020    | 2        | 6238828  |
| 2020    | 3        | 2975454  |
| 2020    | 4        | 231676   |
| 2020    | 5        | 338066   |
| 2020    | 6        | 532299   |
| 2020    | 7        | 775377   |
| 2020    | 8        | 979291   |
| 2020    | 9        | 1314435  |
| 2020    | 10       | 1651184  |
| 2020    | 11       | 1484338  |
| 2020    | 12       | 1438912  |
| 2021    | 1        | 1343958  |
| 2021    | 2        | 1346281  |
| 2021    | 3        | 1894110  |
| 2021    | 4        | 2136558  |
| 2021    | 5        | 2471184  |
| 2021    | 6        | 2797751  |
| 2021    | 7        | 2781928  |
| 2021    | 8        | 35       |
| 2021    | 9        | 3        |
| 2021    | 10       | 3        |
| 2021    | 11       | 5        |
| 2021    | 12       | 5        |
+---------+----------+----------+
*/

--**********************************
-- 4. Table TaxiTripDaily oct. 2019
--**********************************
create table TaxiTripDaily as 
select a.oct_date, COUNT(*) as cnt
FROM (select year(tpep_pickup_datetime) as year, 
month(tpep_pickup_datetime) as month,
to_date(tpep_pickup_datetime) as oct_date from TaxiTripData) a
WHERE a.month = "10" AND a.year = "2019"
GROUP BY a.oct_date
HAVING COUNT(*) > 1
ORDER BY a.oct_date;

/*
the table looks like this
+-------------------------+--------------------+
| taxitripdaily.oct_date  | taxitripdaily.cnt  |
+-------------------------+--------------------+
| 2019-10-01              | 208075             |
| 2019-10-02              | 240951             |
| 2019-10-03              | 254509             |
| 2019-10-04              | 250640             |
| 2019-10-05              | 232890             |
| 2019-10-06              | 193501             |
| 2019-10-07              | 218363             |
| 2019-10-08              | 223187             |
| 2019-10-09              | 225812             |
| 2019-10-10              | 245869             |
| 2019-10-11              | 246587             |
| 2019-10-12              | 225862             |
| 2019-10-13              | 195557             |
| 2019-10-14              | 180699             |
| 2019-10-15              | 234524             |
| 2019-10-16              | 237789             |
| 2019-10-17              | 254535             |
| 2019-10-18              | 257908             |
| 2019-10-19              | 238098             |
| 2019-10-20              | 211767             |
| 2019-10-21              | 209867             |
| 2019-10-22              | 237401             |
| 2019-10-23              | 241976             |
| 2019-10-24              | 252032             |
| 2019-10-25              | 250335             |
| 2019-10-26              | 241925             |
| 2019-10-27              | 205495             |
| 2019-10-28              | 213351             |
| 2019-10-29              | 235679             |
| 2019-10-30              | 239307             |
| 2019-10-31              | 239381             |
+-------------------------+--------------------+
*/

--*******************************
-- 5. Table TaxiTripHourly
--*******************************
create table TaxiTripHourly as
select a.hour, COUNT(*) as cnt
FROM (select hour(tpep_pickup_datetime) as hour from TaxiTripData) a
GROUP BY a.hour
HAVING COUNT(*) > 1
ORDER BY a.hour;

/*
the table looks like this
+----------------------+---------------------+
| taxitriphourly.hour  | taxitriphourly.cnt  |
+----------------------+---------------------+
| 0                    | 3267325             |
| 1                    | 2236882             |
| 2                    | 1530889             |
| 3                    | 1068246             |
| 4                    | 841416              |
| 5                    | 1038186             |
| 6                    | 2472158             |
| 7                    | 4360139             |
| 8                    | 5608896             |
| 9                    | 5779522             |
| 10                   | 5938933             |
| 11                   | 6292796             |
| 12                   | 6754476             |
| 13                   | 6870926             |
| 14                   | 7282800             |
| 15                   | 7342125             |
| 16                   | 6939286             |
| 17                   | 7669574             |
| 18                   | 8240250             |
| 19                   | 7553328             |
| 20                   | 6647059             |
| 21                   | 6428235             |
| 22                   | 5914627             |
| 23                   | 4630331             |
+----------------------+---------------------+
*/

--*******************************
-- 6. Table LocationMap
--*******************************
-- the other csv file mapping pickup location ID and region name
create external table LocationMap 
(LocationID string, Borough string, Zone string, service_zone string)
row format delimited fields terminated by ','
location '/user/pc3095/locationMap/';

--*******************************
-- 7. Table TaxiTripLocation
--*******************************
-- join taxi ride count with pick up location
create table TaxiTripLocation as
SELECT Zone, COUNT(*) as cnt
FROM (SELECT to_date(taxitripdata.tpep_pickup_datetime) as pickup_date,
locationmap.Zone
FROM taxitripdata JOIN locationmap ON (taxitripdata.pickup_location = locationmap.LocationID)) a
GROUP BY Zone
HAVING COUNT(*) > 1
ORDER BY cnt DESC;

--**************************************
-- From Here Join with Weather Data
--**************************************
-- move the weather data to own directory
hadoop fs -mkdir weather
hadoop fs -cp /user/al7527/hiveInput/part-r-00000._COPYING_ weather

--*******************************
-- 8. Table TaxiTrip2019
--*******************************
--create TaxiTrip2019 to join with weather
create table TaxiTrip2019 as 
select a.date_2019, COUNT(*) as cnt
FROM (select year(tpep_pickup_datetime) as year, 
month(tpep_pickup_datetime) as month,
to_date(tpep_pickup_datetime) as date_2019 from TaxiTripData) a
WHERE a.year = "2019"
GROUP BY a.date_2019
HAVING COUNT(*) > 1
ORDER BY a.date_2019;

--*******************************
-- 9. Table weather_raw
--*******************************
-- weather data after MapReduce (copy from al7527)
create external table weather_raw (region string, ele string, time date, val string)
row format delimited fields terminated by ','
location '/user/pc3095/WeatherData/';

--*******************************
-- 10. Table weather_rain
--*******************************
create table weather_rain as
SELECT taxitrip2019.date_2019, taxitrip2019.cnt, weather_raw.ele, weather_raw.val
FROM taxitrip2019
INNER JOIN weather_raw ON taxitrip2019.date_2019=weather_raw.time
WHERE weather_raw.region= 'USW00094728'
AND weather_raw.ele = 'PRCP'
AND weather_raw.val > 10
ORDER BY taxitrip2019.date_2019;

-- count total rainy day
SELECT Count(DISTINCT date_2019, cnt) FROM weather_rain;
/*
+------+
| _c0  |
+------+
| 109  |
+------+
*/

-- count total ride number in rainy day
SELECT SUM(a.cnt) AS Total_Ride_RainyDay
FROM (SELECT DISTINCT date_2019, cnt FROM weather_rain) a;
/*
+----------------------+
| total_ride_rainyday  |
+----------------------+
| 25395084             |
+----------------------+
*/
-- Trip per day rainy day = 25395084/109 = 232982

-- count total ride number in whole year
SELECT SUM(taxitrip2019.cnt) AS Total_Ride_2019
FROM taxitrip2019;
/*
+------------------+
| total_ride_2019  |
+------------------+
| 83641591         |
+------------------+
*/
-- Trip per day = 83641591/365 = 229155

--***************************************
-- In the end we get the ride change 
--***************************************
-- ride diference between rainy day and yearly average  
-- (232982-229155)/229155*100 = 1.67%