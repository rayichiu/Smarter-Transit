-- ==============================NYPD CRIME================================

-- create new table from hdfs file
create external table 
CRIME_all (id string, CMPLNT_FR_DT string, CMPLNT_FR_TM string, CMPLNT_TO_DT string,CMPLNT_TO_TM string, 
OFNS_DESC string, PD_DESC string, LAW_CAT_CD string, BORO_NM string, LOC_OF_OCCUR_DESC string, PREM_TYP_DESC string, 
PARKS_NM string, X_COORD_CD string, Y_COORD_CD string, TRANSIT_DISTRICT string, LATITUDE string, LONGITUDE string, 
PATROL_BORO string, STATION_NM string) row format delimited fields terminated by ';' 
location '/user/hl4674/project/dataset/';

-- create new table from existing table
create table target as 
select id, concat(cmplnt_fr_dt,' ',cmplnt_fr_tm) as fr_dt, concat(cmplnt_to_dt,' ',cmplnt_to_tm) as to_dt,
ofns_desc, pd_desc, law_cat_cd, boro_nm, loc_of_occur_desc, prem_typ_desc, parks_nm,
x_coord_cd as x_coord, y_coord_cd as y_coord, transit_district, latitude, longitude, patrol_boro, station_nm
from crime_all;

-- change data type
alter table target change fr_dt fr_dt timestamp;
alter table target change to_dt to_dt timestamp;

-- ===final table===
create table target_new as
select * from target 
where year(fr_dt) is not null and year(fr_dt) > 2005;

-- data sampling in presentation
select fr_dt as cmplnt_time, law_cat_cd as law_cat, boro_nm, prem_typ_desc as prem_type,
parks_nm, transit_district, station_nm from target_new
where boro_nm <> "" and prem_typ_desc <> "" 
and parks_nm <> "" and station_nm <> "" limit 10;

select fr_dt as cmplnt_time, law_cat_cd as law_cat, boro_nm, 
prem_typ_desc as prem_type, parks_nm, transit_district, station_nm 
from target_new
where boro_nm <> "" and parks_nm not in ("", "NA") limit 10;


-- ==============================WEATHER================================

/*********CMD*********/
-- move the weather data to own directory
hadoop fs -mkdir weather
hadoop fs -cp /user/al7527/hiveInput/part-r-00000._COPYING_ weather
/********************/

-- create new table from hdfs file
create external table p1 (region string, ele string, time date, val string)
row format delimited fields terminated by ','
location '/user/hl4674/weather/';

alter table p1 rename to weather;

-- create another crime table to meet the column of weather table
 create table target_w as
 select * from crime_all where cmplnt_fr_dt is not null and
 substring(cmplnt_fr_dt, 1, 4) between 2006 and 2021;