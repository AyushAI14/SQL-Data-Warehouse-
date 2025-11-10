-- Procedure: bronze.load_bronze()
-- Purpose: Load Bronze layer by truncating raw tables and bulk-copying CSV files.
-- Actions:
--   • Truncate each bronze table
--   • COPY data from server-side import directory
--   • Log load time per table + total duration (RAISE NOTICE)
-- Files expected in: /var/lib/postgresql/imports/Datasets/
-- Sources loaded: CRM (cust, prd, sales) + ERP (cust, loc, cat)
-- Run with: CALL bronze.load_bronze();




CREATE OR REPLACE PROCEDURE bronze.load_bronze ()
LANGUAGE plpgsql
AS $$
DECLARE 
    global_start TIMESTAMP;
    t_start TIMESTAMP;
    t_end   TIMESTAMP;
BEGIN
    global_start := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Bronze Layer';
    RAISE NOTICE '================================================';

    -----------------------------------------------------------------------
    -- CRM: Customer
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating crm_cust_info';
    TRUNCATE TABLE bronze.crm_cust_info;

    RAISE NOTICE 'Loading crm_cust_info';
    COPY bronze.crm_cust_info
        FROM '/var/lib/postgresql/imports/Datasets/source_crm/cust_info.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

    t_end := clock_timestamp();
    RAISE NOTICE 'crm_cust_info loaded in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);

    -----------------------------------------------------------------------
    -- CRM: Product
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating crm_prd_info';
    TRUNCATE TABLE bronze.crm_prd_info;

    RAISE NOTICE 'Loading crm_prd_info';
    COPY bronze.crm_prd_info
        FROM '/var/lib/postgresql/imports/Datasets/source_crm/prd_info.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

    t_end := clock_timestamp();
    RAISE NOTICE 'crm_prd_info loaded in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);

    -----------------------------------------------------------------------
    -- CRM: Sales
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating crm_sales_details';
    TRUNCATE TABLE bronze.crm_sales_details;

    RAISE NOTICE 'Loading crm_sales_details';
    COPY bronze.crm_sales_details
        FROM '/var/lib/postgresql/imports/Datasets/source_crm/sales_details.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

    t_end := clock_timestamp();
    RAISE NOTICE 'crm_sales_details loaded in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);

    -----------------------------------------------------------------------
    -- ERP: Customer
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating erp_CUST_AZ12';
    TRUNCATE TABLE bronze.erp_CUST_AZ12;

    RAISE NOTICE 'Loading erp_CUST_AZ12';
    COPY bronze.erp_CUST_AZ12
        FROM '/var/lib/postgresql/imports/Datasets/source_erp/CUST_AZ12.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

    t_end := clock_timestamp();
    RAISE NOTICE 'erp_CUST_AZ12 loaded in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);

    -----------------------------------------------------------------------
    -- ERP: Location
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating erp_LOC_A101';
    TRUNCATE TABLE bronze.erp_LOC_A101;

    RAISE NOTICE 'Loading erp_LOC_A101';
    COPY bronze.erp_LOC_A101
        FROM '/var/lib/postgresql/imports/Datasets/source_erp/LOC_A101.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

    t_end := clock_timestamp();
    RAISE NOTICE 'erp_LOC_A101 loaded in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);

    -----------------------------------------------------------------------
    -- ERP: Product Category
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating erp_PX_CAT_G1V2';
    TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;

    RAISE NOTICE 'Loading erp_PX_CAT_G1V2';
    COPY bronze.erp_PX_CAT_G1V2
        FROM '/var/lib/postgresql/imports/Datasets/source_erp/PX_CAT_G1V2.csv'
        WITH (FORMAT csv, HEADER, DELIMITER ',');

    t_end := clock_timestamp();
    RAISE NOTICE 'erp_PX_CAT_G1V2 loaded in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);

    -----------------------------------------------------------------------
    -- Total runtime
    -----------------------------------------------------------------------
    RAISE NOTICE '=========== Bronze Load Completed ===========';

    RAISE NOTICE 'Total Bronze Layer Load Time: % ms', 
        EXTRACT(MILLISECOND FROM clock_timestamp() - global_start);
END;
$$;
