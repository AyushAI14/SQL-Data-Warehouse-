/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

===============================================================================
*/

/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
*/

CALL silver.load_silver();  -- for executing the procedure

CREATE OR REPLACE PROCEDURE silver.load_silver ()
LANGUAGE plpgsql
AS $$
DECLARE 
    global_start TIMESTAMP;
    t_start TIMESTAMP;
    t_end   TIMESTAMP;
BEGIN
    global_start := clock_timestamp();

    RAISE NOTICE '================================================';
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '================================================';

    -----------------------------------------------------------------------
    -- CRM: Customer
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating silver.crm_cust_info';
    TRUNCATE TABLE silver.crm_cust_info;

    RAISE NOTICE 'Inserting silver.crm_cust_info';
    INSERT INTO silver.crm_cust_info(
        cst_id, cst_key, cst_firstname, cst_lastname, cst_gndr, cst_marital_status, cst_create_date
    )
    SELECT 
        cst_id,
        cst_key,
        TRIM(cst_firstname), -- removing unwanted space
        TRIM(cst_lastname),  -- removing unwanted space
        CASE 
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female' -- standardizing the data
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'unknown'
        END AS cst_gndr,
        CASE 
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single' -- standardizing the data
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'unknown'
        END AS cst_marital_status,
        cst_create_date
    FROM bronze.crm_cust_info;
    
    t_end := clock_timestamp();
    RAISE NOTICE 'crm_cust_info inserted in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);
    
    -----------------------------------------------------------------------
    -- CRM: Product
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating silver.crm_prd_info';
    TRUNCATE TABLE silver.crm_prd_info;

    RAISE NOTICE 'Inserting silver.crm_prd_info';
    
    INSERT INTO silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LENGTH(prd_key)) AS prd_key,
        prd_nm,
        COALESCE(prd_cost, 0) AS prd_cost,
        CASE 
            WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,
        CAST(prd_start_dt AS DATE) AS prd_start_dt,
        CAST(
            LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
            AS DATE
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;

    t_end := clock_timestamp();
    RAISE NOTICE 'crm_prd_info inserted in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);
    
    -----------------------------------------------------------------------
    -- CRM: Sales (Uses CTE for multi-step calculation)
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating silver.crm_sales_details';
    TRUNCATE TABLE silver.crm_sales_details;

    RAISE NOTICE 'Inserting silver.crm_sales_details';

    INSERT INTO silver.crm_sales_details (
        sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
    )
    WITH sales_clean AS (
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            -- Date Cleaning
            CASE WHEN sls_order_dt = 0 OR LENGTH(sls_order_dt::TEXT) != 8 THEN NULL ELSE TO_DATE(sls_order_dt::TEXT, 'YYYYMMDD') END AS sls_order_dt,
            CASE WHEN sls_ship_dt = 0 OR LENGTH(sls_ship_dt::TEXT) != 8 THEN NULL ELSE TO_DATE(sls_ship_dt::TEXT, 'YYYYMMDD') END AS sls_ship_dt,
            CASE WHEN sls_due_dt = 0 OR LENGTH(sls_due_dt::TEXT) != 8 THEN NULL ELSE TO_DATE(sls_due_dt::TEXT, 'YYYYMMDD') END AS sls_due_dt,
            sls_quantity,
            sls_price,
            -- STEP 1: Calculate the corrected sales value (corrected_sales)
            CASE 
                WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
                    THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS corrected_sales
        FROM
            bronze.crm_sales_details
    )
    -- STEP 2: Select final values, using corrected_sales to derive price
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        corrected_sales AS sls_sales, -- Use the calculated sales
        sls_quantity,
        CASE 
            WHEN sls_price IS NULL OR sls_price <= 0  
                THEN corrected_sales / NULLIF(sls_quantity, 0) -- Derive price using corrected_sales
            ELSE sls_price 
        END AS sls_price
    FROM
        sales_clean;
        
    t_end := clock_timestamp();
    RAISE NOTICE 'crm_sales_details inserted in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);

    -----------------------------------------------------------------------
    -- ERP: Customer
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating silver.erp_CUST_AZ12';
    TRUNCATE TABLE silver.erp_CUST_AZ12;
    RAISE NOTICE 'Inserting silver.erp_cust_az12';

    INSERT INTO silver.erp_cust_az12 (cid, bdate, gen)
    SELECT
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LENGTH(cid)) -- Remove 'NAS' prefix if present
            ELSE cid
        END AS cid, 
        CASE
            WHEN bdate > NOW() THEN NULL
            ELSE bdate
        END AS bdate, -- Set future birthdates to NULL
        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
            ELSE 'n/a'
        END AS gen -- Normalize gender values and handle unknown cases
    FROM bronze.erp_cust_az12;

    t_end := clock_timestamp();
    RAISE NOTICE 'erp_CUST_AZ12 inserted in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);
    
    -----------------------------------------------------------------------
    -- ERP: Location
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating silver.erp_LOC_A101';
    TRUNCATE TABLE silver.erp_LOC_A101;
    RAISE NOTICE 'Inserting silver.erp_LOC_A101';

    -- Cleaned erp_loc_a101 data and inserting to silver
    INSERT INTO silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid, 
        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE TRIM(cntry)
        END AS cntry -- Normalize and Handle missing or blank country codes
    FROM bronze.erp_loc_a101;
    
    t_end := clock_timestamp();
    RAISE NOTICE 'erp_LOC_A101 loaded in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);
    
    -----------------------------------------------------------------------
    -- ERP: Product Category
    -----------------------------------------------------------------------
    t_start := clock_timestamp();
    RAISE NOTICE 'Truncating silver.erp_PX_CAT_G1V2';
    TRUNCATE TABLE silver.erp_PX_CAT_G1V2; -- Corrected table target to silver

    RAISE NOTICE 'Inserting silver.erp_PX_CAT_G1V2';

    INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
    
    t_end := clock_timestamp();
    RAISE NOTICE 'erp_PX_CAT_G1V2 loaded in % ms', EXTRACT(MILLISECOND FROM t_end - t_start);

    -----------------------------------------------------------------------
    -- Total runtime
    -----------------------------------------------------------------------
    RAISE NOTICE '=========== Bronze Load Completed ===========';

    RAISE NOTICE 'Total Sliver Layer Insertion Time: % ms', 
        EXTRACT(MILLISECOND FROM clock_timestamp() - global_start);
END;
$$;