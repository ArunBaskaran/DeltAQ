
/*--------------------Queries for table creation---------------*/

CREATE TABLE if not exists usa (time TIMESTAMPTZ, week integer, day integer, zip integer, parameter varchar, unit varchar, value double precision) ;

SELECT create_hypertable('usa', 'zip', 'parameter', 'time');

CREATE TABLE if not exists uszipcodes (index bigint, zip bigint, lat double precision, long double precision, city varchar, state_id varchar, state_name varchar) ; 

CREATE TABLE if not exists lat_long_zip_us (latitude double precision, longitude double precision zip bigint) ;



