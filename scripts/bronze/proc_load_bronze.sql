
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	BEGIN TRY
	DECLARE @start_date DATETIME,@end_date DATETIME, @batch_start_date DATETIME, @batch_end_date DATETIME;
		PRINT '==============================================';
		PRINT 'Loading Bronze Layer';
		PRINT '==============================================';

		PRINT '----------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '----------------------------------------------';

		SET @batch_start_date = GETDATE()

		SET  @start_date = GETDATE()
		PRINT '>> Truncate Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH
		(
			FIRSTROW=2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE()
		PRINT 'Load Duration:' + CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + ' second'
		PRINT '---------------';

		SET @start_date = GETDATE()
		PRINT '>> Truncate Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info

		PRINT '>> Inserting Data Into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_date = GETDATE()
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + ' second';
		PRINT '----------------';

		SET @start_date = GETDATE()
		PRINT '>> Truncate Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details

		PRINT '>> Inserting Data Into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE()
		PRINT 'Load Duration: ' + CAST (DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + ' second';
		PRINT '-----------------';

		PRINT '----------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '----------------------------------------------';

		SET @start_date = GETDATE()
		PRINT '>> Truncate Table: bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR =',',
			TABLOCK
		);
		SET @end_date = GETDATE()
		PRINT 'Load Duration: ' + CAST(DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + ' second';
		PRINT '---------------';

		SET @start_date = GETDATE()
		PRINT '>> Truncate Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101

		PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE()
		PRINT 'Load Duration: ' + CAST (DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + ' second';
		PRINT '-----------------';

		SET @start_date = GETDATE()
		PRINT '>> Truncate Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\sql-data-warehouse-project-main\datasets\source_erp\px_cat_g1v2.csv'
		WITH
		(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_date = GETDATE()
		PRINT 'Load Duration: ' + CAST (DATEDIFF(second,@start_date,@end_date) AS NVARCHAR) + ' second';
		PRINT '-----------------';

		SET @batch_end_date = GETDATE()
		PRINT '==================================================================';
		PRINT 'Loading Bronze Leyar is Completed';
		PRINT '    -Total Duration Time: ' + CAST(DATEDIFF(SECOND,@batch_start_date,@batch_end_date) AS NVARCHAR) + ' second';
		PRINT '==================================================================';

	END TRY
	BEGIN CATCH
		PRINT '==============================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + Error_Message();
		PRINT 'Error Number' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State' + CAST (ERROR_STATE() AS NVARCHAR)
		PRINT '==============================================';
	END CATCH
END
