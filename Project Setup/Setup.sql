 CREATE CATALOG IF NOT EXISTS flights_catalog
     MANAGED LOCATION 's3://databricks-flights-project/';

----------
 CREATE SCHEMA IF NOT EXISTS flights_catalog.raw;

----------
 CREATE EXTERNAL VOLUME IF NOT EXISTS flights_catalog.raw.raw_flights_vol
     LOCATION 's3://databricks-flights-project/raw_data/';

----------
 CREATE SCHEMA IF NOT EXISTS flights_catalog.bronze;
 CREATE SCHEMA IF NOT EXISTS flights_catalog.silver;
 CREATE SCHEMA IF NOT EXISTS flights_catalog.gold;

----------
 CREATE VOLUME IF NOT EXISTS flights_catalog.bronze.pipeline_state;