-----------------------------------------------------------------------------------------------------------------------
------------------Following Script shows the steps taken to load raw data into Bronze layer----------------------------
------------------Creating a stored procedure to execute the loading phase and tracking time take load-----------------

IF OBJECT_ID ('bronze.crm_cust_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_cust_info;
CREATE Table bronze.crm_cust_info(
	cst_id INT,
	cst_key NVARCHAR(50),
	cst_firstname NVARCHAR(50),
	cst_lastname NVARCHAR(50),
	cst_marital_status NVARCHAR(50),
	cst_gender NVARCHAR(50),
	cst_create_date DATE
);
GO

IF OBJECT_ID ('bronze.crm_prd_info','U') IS NOT NULL 
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
	prd_id INT,
	prd_key NVARCHAR(50),
	prd_nm NVARCHAR(50),
	prd_cost INT,
	prd_line NVARCHAR(50),
	prd_start_dt DATETIME,
	prd_end_dt DATETIME
);
GO

IF OBJECT_ID ('bronze.crm_sales_details','U') IS NOT NULL 
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
	sls_ord_num NVARCHAR(50),
	sls_prd_key NVARCHAR(50),
	sls_cust_id INT,
	sls_order_dt INT,
	sls_ship_dt INT,
	sls_due_dt INT,
	sls_sales INT,
	sls_quantity INT,
	sls_price INT
);
GO

IF OBJECT_ID ('bronze.erp_cust_aZ12','U') IS NOT NULL 
	DROP TABLE bronze.erp_cust_aZ12;
CREATE TABLE bronze.erp_cust_aZ12(
	cid NVARCHAR(50),
	bdate DATE,
	gen NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_loc_a101','U') IS NOT NULL 
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
	cid NVARCHAR(50),
	cntry NVARCHAR(50)
);
GO

IF OBJECT_ID ('bronze.erp_px_cat_g1v2','U') IS NOT NULL 
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
	id NVARCHAR(50),
	cat NVARCHAR(50),
	subcat NVARCHAR(50),
	maintenance NVARCHAR(50)
);
GO 
EXEC bronze.load_bronze

GO
CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	
	BEGIN TRY
		SET @batch_start_time = GETDATE()
		print'-----------------------';
		PRINT 'Loading Bronze Layer';
		print '-----------------------';

		print'-----------------------';
		PRINT 'Loading FROM CRM';
		print '-----------------------';

		-- BULK INSERT: INSERTS ALL DATA AT ONCE
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>> Inserting Data Into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		-- INSERT DATA PATH
		FROM 'D:\Datasets\Source_crm\cust_info.csv'
		-- DETERMINE SOME INSTRUCTIONS FOR LOADING # BEGIN FROM SECOND ROW ,DELIMITER,LOCKING THE TABLE WHILE LOADING
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds';
		PRINT '-------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT 'Inserting Data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'D:\Datasets\Source_crm\prd_info.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds';
		PRINT '-------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT 'Inserting Data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'D:\Datasets\Source_crm\sales_details.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds';
		PRINT '-------------------------';


		PRINT 'Loading FROM ERP';
		print '-----------------------';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_cust_aZ12';
		TRUNCATE TABLE bronze.erp_cust_aZ12;

		PRINT 'Inserting Data into:bronze.erp_cust_aZ12';
		BULK INSERT bronze.erp_cust_aZ12
		FROM 'D:\Datasets\Source_erp\CUST_AZ12.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds';
		PRINT '-------------------------';
		
		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT 'Inserting Data into:bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'D:\Datasets\Source_erp\LOC_A101.csv'
		WITH(
			FIRSTROW = 2, 
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds';
		PRINT '-------------------------';

		SET @start_time = GETDATE();
		PRINT 'Truncating Table: bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;

		PRINT 'Inserting Data into:bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'D:\Datasets\Source_erp\PX_CAT_G1V2.csv'
		WITH(
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR) +' seconds';
		PRINT '-------------------------';

		SET @batch_end_time = GETDATE();
		PRINT '>>Batch Load Duration: ' +CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR) +' seconds';
		PRINT '-------------------------';

	END TRY
	BEGIN CATCH
		PRINT '------------------------------------------'
		PRINT '	Error Occured during loading bronze layer'
		 PRINT 'Error Meassage'+ ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '------------------------------------------'
	END CATCH

END 
