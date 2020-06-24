
/*--------------------Queries for table creation---------------*/

CREATE TABLE if not exists openaq_clean (city varchar, country varchar, location varchar, mobile varchar, parameter varchar, sourceName varchar, 
 sourceType varchar, unit varchar, value double precision, dt_utc date, dt_local date, latitude double precision, longitude double precision, 
 ap_units varchar, ap_value double precision, week integer) ;

CREATE TABLE if not exists uszipcodes (index bigint, zip bigint, lat double precision, long double precision, city varchar, state_id varchar, state_name varchar) ; 

CREATE TABLE if not exists lat_long_zip_us (latitude double precision, longitude double precision zip bigint) ;



