-- PROJECT: Data Migration & Ingestion (AWS S3 - Snowflake)


-- DATABASE SETUP
CREATE DATABASE airdb;
CREATE SCHEMA airsch;

-- STORAGE INTEGRATION
CREATE OR REPLACE STORAGE INTEGRATION aws_integration 
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = 'S3'
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = '<role-arn>'
STORAGE_ALLOWED_LOCATIONS = ('s3://<bucket>/<path>/');

-- Verify integration
SHOW INTEGRATIONS;
DESC INTEGRATION aws_integration;

-- Grant usage access
GRANT USAGE ON INTEGRATION aws_integration TO ROLE accountadmin;

-- FILE FORMAT SETUP
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
NULL_IF = ('NULL', 'null')
EMPTY_FIELD_AS_NULL = TRUE;

-- CREATE EXTERNAL STAGE
CREATE OR REPLACE STAGE my_s3_stage
URL = 's3://<bucket>/<path>/'
STORAGE_INTEGRATION = aws_integration
FILE_FORMAT = my_csv_format;

-- CREATE EXTERNAL STAGE
LIST @my_s3_stage;

--  CREATE STAGING TABLE
CREATE OR REPLACE TEMP TABLE aircraft_temp (
  Airline STRING,
  Aircraft_Model STRING,
  Registration_ID STRING,
  Year_Built INT,
  Seats INT,
  Engine_Type STRING,
  Range_km INT,
  Fuel_Burn_L_hr INT,
  Status STRING
);

-- DATA VALIDATION

-- Validate incoming data before loading
COPY INTO aircraft_temp
FROM @my_s3_stage
VALIDATION_MODE = 'RETURN_ERRORS';


-- DATA LOADING

-- Load data into staging table with error handling
COPY INTO aircraft_temp
FROM @my_s3_stage
ON_ERROR = 'CONTINUE';

-- Remove duplicates
SELECT *
FROM aircraft_temp
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY Registration_ID
  ORDER BY Year_Built DESC
) = 1;

-- Handle NULL values
SELECT
  Airline,
  Aircraft_Model,
  Registration_ID,
  COALESCE(Year_Built, 0) AS Year_Built,
  COALESCE(Seats, 0) AS Seats,
  COALESCE(Engine_Type, 'Unknown') AS Engine_Type,
  COALESCE(Range_km, 0) AS Range_km,
  COALESCE(Fuel_Burn_L_hr, 0) AS Fuel_Burn_L_hr,
  COALESCE(Status, 'Unknown') AS Status
FROM aircraft_temp;

-- FINAL MIGRATED TABLE
CREATE OR REPLACE TABLE aircraft_final AS
SELECT * FROM air_cleaned;