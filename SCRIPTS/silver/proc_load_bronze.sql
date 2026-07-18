/*
===============================================================================
Stored Procedure: Load silver Layer (bronze -> silver)
===============================================================================
Script Purpose: 
    This stored procedure performs the ETL(extract,transform,load) process to 
    load data into 'silver'  schema tables from bronze tables 

    It performs the following actions:
    - Truncates the silver tables before loading data from bronze tables 
    - inserts transformed and cleaned data from bronze tables to silver tables 

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver  AS 
BEGIN
     DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
     BEGIN TRY 
          SET @batch_start_time = GETDATE()
          PRINT'============================================================'
          PRINT'          LOADING SILVER LAYER'
          PRINT'============================================================'
          
          PRINT'============================================================'
          PRINT'            LOADING CRM TABLES'
          PRINT'============================================================='
          
          ----LOADING silver.crm_cust_info----
          SET @start_time = GETDATE()
          PRINT' <<TRUNCATING TABLE :silver.crm_cust_info'
          TRUNCATE TABLE silver.crm_cust_info 
          PRINT' <<INSERTING DATA INTO:silver.crm_cust_info'
          INSERT INTO  silver.crm_cust_info (
                          cst_id ,
                          cst_key,
                          cst_firstname,
                          cst_lastname,
                          cst_marital_status,
                          cst_gndr,
                          cst_create_date
                          )
                SELECT cst_id ,
                       cst_key,
                       TRIM(cst_firstname) as cst_firstname,
                       TRIM(cst_lastname) as  cst_lastname,
                CASE WHEN TRIM(UPPER(cst_marital_status)) = 'M' 
                     THEN 'Married'
                     WHEN TRIM(UPPER(cst_marital_status)) = 'S'
                     THEN 'single'
                     ELSE 'n/a'
                END AS cst_marital_status ,
                CASE WHEN TRIM(UPPER(cst_gndr)) = 'M' 
                     THEN 'male'
                     WHEN TRIM(UPPER(cst_gndr)) = 'F'
                     THEN 'female'
                     ELSE 'n/a'
                END AS cst_gndr,
                cst_create_date
                    FROM (
                         SELECT * 
                          FROM
                             (
                                 SELECT *, ROW_NUMBER() OVER(partition by cst_id order by cst_create_date ) as flag 
                                 FROM bronze.crm_cust_info 
                                 WHERE cst_id is not null
                                        ) t 
                                               WHERE  1 =  flag 
                                                                       ) t1
          
            SET @end_time = GETDATE()
            PRINT'<<LOAD DURATION:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'SECONDS'
            PRINT'---------------------------------------------------------------'

            --LOADING silver.crm_prd_info--
            SET @start_time=GETDATE()
            PRINT' <<TRUNCATING TABLE:silver.crm_prd_inf '
            TRUNCATE TABLE silver.crm_prd_info
            PRINT' <<INSERTING DATA INTO:silver.crm_prd_info'
            INSERT INTO  silver.crm_prd_info (
                    prd_id ,      
                    cat_id  ,     
                    prd_key ,   
                    prd_nm  ,    
                    prd_cost ,    
                    prd_line  ,   
                    prd_start_dt, 
                    prd_end_dt 
                )
                SELECT prd_id,
                       REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id , -- EXTRACT CATEGORY ID 
                       SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key, -- EXTRACT PRODUCT KEY 
                       prd_nm,
                       ISNULL(prd_cost,0) as prd_cost,
                       CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'mountain'
                            WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'road'
                            WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'othersales'
                            WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'touring'
                            ELSE 'N/A' 
                            END as prd_line,  -- MAP PRODUCT LINE CODES TO DESCRIPTIVE VALUES 
                        prd_start_dt  as prd_start_dt,
                        DATEADD(day,-1,LEAD(prd_start_dt) OVER(partition by prd_key order by prd_start_dt ))  as prd_end_dt  -- CALCULATE END DATE AS 
                FROM bronze.crm_prd_info                               -- ONE DAY BEFORE NEXT  START DATE 
                SET @end_time= GETDATE()
                PRINT'LOAD DURATION:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +'SECONDS' 
                PRINT'--------------------------------------------------------------'

                --LOADING silver.crm_sales_details--
                SET @start_time=GETDATE();
                PRINT' <<TRUNCATING TABLE:silver.crm_sales_details'
                TRUNCATE TABLE silver.crm_sales_details
                PRINT' <<INSERTING DATA INTO:silver.crm_sales_details'
                INSERT INTO silver.crm_sales_details (
                               sls_ord_num,
                               sls_prd_key,
                               sls_cust_id,
                               sls_order_dt,
                               sls_ship_dt,
                               sls_due_dt,
                               sls_sales,
                               sls_quantity,
                               sls_price 
                               )
                SELECT sls_ord_num,
                       sls_prd_key,
                       sls_cust_id,
                CASE WHEN sls_order_dt <= 0 or len(sls_order_dt) != 8  THEN NULL 
                    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)   
                    END AS sls_order_dt,
                CASE WHEN sls_ship_dt <= 0 or len(sls_ship_dt) != 8  THEN NULL 
                    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)   
                    END AS sls_ship_dt,
                CASE WHEN sls_due_dt <= 0 or len(sls_due_dt) != 8  THEN NULL 
                    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)   
                    END AS sls_due_dt,
                CASE WHEN  sls_sales is null OR sls_sales <= 0 or sls_sales != sls_quantity * ABS(sls_price)  THEN sls_quantity * sls_price 
                     ELSE  sls_sales 
                     END AS sls_sales ,
                        sls_quantity,
                CASE WHEN sls_price is null OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0)  
                     ELSE sls_price 
                     END AS sls_price
                FROM bronze.crm_sales_details 
                SET @end_time=GETDATE();
                PRINT'LOAD DURATION:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR)  + 'seconds'
                PRINT'--------------------------------------------------------------'

                --LOADING silver.erp_cust_az12--
                SET @start_time=GETDATE()
                PRINT' <<TRUNCATE TABLE:silver.erp_cust_az12'
                TRUNCATE TABLE silver.erp_cust_az12
                PRINT' <<INSERTING DATA INTO:silver.erp_cust_az12'
                INSERT INTO silver.erp_cust_az12 (cid,bdate,gen)
                SELECT 
                       CASE   WHEN cid LIKE 'NAS%' THEN SUBSTRING(CID,4,LEN(cid))
                              ELSE cid 
                       END AS cid, 
                       CASE   WHEN GETDATE() <  bdate THEN NULL 
                              ELSE bdate 
                       END AS bdate, 
                       CASE   WHEN upper(trim(gen)) IN ('F','FEMALE') THEN 'Female'
                              WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
                              ELSE 'n/a'
                       END AS  gen
                FROM bronze.erp_cust_az12
                SET @end_time = GETDATE()
                PRINT'LOAD DURATION:'+CAST(DATEDIFF(SECOND,@start_time,@end_time) AS  NVARCHAR)+'SECONDS'
                PRINT'--------------------------------------------------------------'

                --LOADING silver.erp_loc_a101--
                SET @start_time= GETDATE()
                PRINT' <<TRUNCATING TABLE:silver.erp_loc_a101'
                TRUNCATE TABLE silver.erp_loc_a101
                PRINT' <<INSERTING DATA INTO:silver.erp_loc_a101'
                INSERT INTO silver.erp_loc_a101(cid,cntry)
                SELECT 
                REPLACE(cid,'-','') as cid,
                CASE WHEN TRIM(cntry) in ('US','USA') THEN  'United States'
                     WHEN TRIM(cntry) IS NULL OR cntry = '' THEN 'N/A'
                     WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                     ELSE TRIM(cntry) 
                     END AS cntry 
                FROM bronze.erp_loc_a101
                SET @end_time=GETDATE()
                PRINT'LOAD DURATION:'+ CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) + 'SECONDS'
                PRINT'---------------------------------------------------------------'

                --LOADING silver.erp_px_cat_g1v2--
                SET @start_time=GETDATE()
                PRINT' TRUNCATING TABLE:silver.erp_px_cat_g1v2'
                TRUNCATE TABLE silver.erp_px_cat_g1v2
                PRINT' <<INSERTING DATA INTO:silver.erp_px_cat_g1v2'
                INSERT INTO silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
                SELECT id,
                       cat,
                       subcat,
                       maintenance
                FROM  bronze.erp_px_cat_g1v2
                SET @end_time=GETDATE()
                PRINT'LOAD DURATION:' + CAST(DATEDIFF(SECOND,@start_time,@end_time) AS NVARCHAR) +'SECONDS'
                PRINT'----------------------------------------------------------------'

                SET @batch_end_time = GETDATE()
                PRINT'================================================================='
                PRINT'           LOADING SILVER LAYER IS COMPLETED'
                PRINT'================================================================='
                PRINT'TOTAL LOAD DURATION:' + CAST(DATEDIFF(SECOND,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'second'
         END TRY 
         BEGIN CATCH 
                PRINT'====================================================='
                PRINT'           ERROR OCCURED DURING LOADING SILVER LAYER'
                PRINT'ERROR MESSAGE' + ERROR_MESSAGE();
                PRINT'ERROR MESSAGE' + CAST(ERROR_MESSAGE() AS NVARCHAR);
                PRINT'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
                PRINT'====================================================='

                END CATCH 
                END 
