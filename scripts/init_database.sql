-- Access Master DB where I can create new DBS
USE Master;
GO
-- Create new DB DataWarehouse
Create DATABASE DataWarehouse;
GO
--Access DataWarehouse
USE DataWarehouse;
GO
--Create Schema for each layer to develop each layer separately
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO


