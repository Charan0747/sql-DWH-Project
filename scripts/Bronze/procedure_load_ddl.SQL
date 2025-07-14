/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'Bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Bronze.load_bronze;
===============================================================================
*/
CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME , @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT('==============================================');
		PRINT('Loading Bronze Layer');
		PRINT('==============================================');

		PRINT('-------------------------------------------------');
		PRINT('Loading The CRM tables');
		PRINT('-------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>>Truncating the Tables:Bronze.crm_cust_info');
		TRUNCATE TABLE Bronze.crm_cust_info; -- FULL LOAD 
		PRINT('>> Innserting the Data into :Bronze.crm_cust_info');
		BULK INSERT Bronze.crm_cust_info 
		FROM 'C:\Users\cherr\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR )+ 'seconds');
		PRINT('----------------');

		SET @start_time = GETDATE();
		PRINT('>>Truncating the Tables:Bronze.crm_prd_info');
		TRUNCATE TABLE Bronze.crm_prd_info; 
		PRINT('>> Innserting the Data into :Bronze.crm_prd_info');
		BULK INSERT Bronze.crm_prd_info 
		FROM 'C:\Users\cherr\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds');
		PRINT('----------------');

		SET @start_time = GETDATE();
		PRINT('>>Truncating the Tables:Bronze.crm_sales_details');
		TRUNCATE TABLE Bronze.crm_sales_details; -- FULL LOAD 
		PRINT('>> Innserting the Data into :Bronze.crm_sales_details');
		BULK INSERT Bronze.crm_sales_details 
		FROM 'C:\Users\cherr\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds');
		PRINT('----------------');

		PRINT('-------------------------------------------------');
		PRINT('Loading The ERP tables');
		PRINT('-------------------------------------------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncating the Tables :Bronze.erp_cust_az12');
		TRUNCATE TABLE Bronze.erp_cust_az12; -- FULL LOAD 
		PRINT('>> Innserting the Data into :Bronze.erp_cust_az12');
		BULK INSERT Bronze.erp_cust_az12 
		FROM 'C:\Users\cherr\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds');
		PRINT('----------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncating the Tables :Bronze.erp_loc_a101');
		TRUNCATE TABLE Bronze.erp_loc_a101; -- FULL LOAD 
		PRINT('>> Innserting the Data into :Bronze.erp_loc_a101');
		BULK INSERT Bronze.erp_loc_a101 
		FROM 'C:\Users\cherr\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds');
		PRINT('----------------');

		SET @start_time = GETDATE();
		PRINT('>> Truncating the Tables :Bronze.erp_px_cat_g1v2');
		TRUNCATE TABLE Bronze.erp_px_cat_g1v2; -- FULL LOAD 
		PRINT('>> Innserting the Data into :Bronze.erp_px_cat_g1v2');
		BULK INSERT Bronze.erp_px_cat_g1v2 
		FROM 'C:\Users\cherr\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT('>> Load Duration:' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) + 'seconds');
		PRINT('----------------');

		SET @batch_end_time = GETDATE();
		PRINT('============================================');
		PRINT('Batch Loading Of Bronze Layer is Completed');
		PRINT('>> Time Duration Bronze Layer Loading:' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) + 'seconds');
		PRINT('============================================');

	END TRY
	
	BEGIN CATCH
		PRINT('================================================================');
		PRINT('ERROR OCCURED DURING LODING OF BRONZE LAYER');
		PRINT('ERROR MESSAGE' + ERROR_MESSAGE());
		PRINT('ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR));
		PRINT('ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR));
		PRINT('================================================================');
	END CATCH
END;
