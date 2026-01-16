-- =============================================================
-- Procedure: bronze.load_bronze
-- Purpose:   Create medallion schemas + bronze tables (if needed),
--            truncate, and load CSVs from /datasets.
-- Start/Stop Docker Container:
--            Stop: docker compose down -v
--            Start: docker compose up -d
-- =============================================================

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
  v_start       timestamptz := clock_timestamp();
  v_step_start  timestamptz;
  v_rows        bigint;
BEGIN
  RAISE NOTICE 'Starting bronze load...';

  -- --------------------
  -- Schemas
  -- --------------------
  CREATE SCHEMA IF NOT EXISTS bronze;
  CREATE SCHEMA IF NOT EXISTS silver;
  CREATE SCHEMA IF NOT EXISTS gold;

  -- --------------------
  -- Tables (Bronze)
  -- --------------------




  -- --------------------
  -- Truncate for reload
  -- --------------------
  RAISE NOTICE 'Truncating bronze tables...';
  TRUNCATE TABLE
    bronze.

  -- --------------------
  -- Load: File
  -- --------------------
  v_step_start := clock_timestamp();
  EXECUTE format($f$
    COPY bronze.crm_cust_info
    FROM %L
    WITH (FORMAT csv, HEADER true, DELIMITER ',', NULL '', ENCODING 'UTF8')
  $f$, '/datasets/ add_file.csv');

  SELECT count(*) INTO v_rows FROM bronze. add_file;
  RAISE NOTICE 'Loaded bronze. add_file: % rows (%.3f sec)',
    v_rows, EXTRACT(epoch FROM (clock_timestamp() - v_step_start));


END;
$$;

-- Run it:
CALL bronze.load_bronze();
