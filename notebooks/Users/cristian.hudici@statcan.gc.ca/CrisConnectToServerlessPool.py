# Databricks notebook source
import adal # Active Directory Authentication Library
keyvault = "devsandbox-kv1"
 
tenant = dbutils.secrets.get(scope = keyvault, key = "TenantID")
login_url = "https://login.microsoftonline.com"
authority_url = (login_url + '/' + tenant)
 
context = adal.AuthenticationContext(authority_url, timeout=None)
 
access_token = context.acquire_token_with_client_credentials(
    "https://database.windows.net/",
    dbutils.secrets.get(scope = keyvault, key = "DataBricksClientID"),
    dbutils.secrets.get(scope = keyvault, key = "DataBricksSecret"))
 
jdbcHostname = "dev-sandbox-syn-ondemand.sql.azuresynapse.net"
jdbcDatabase = "testingdb"
jdbcPort = 1433
 
jdbcUrl = "jdbc:sqlserver://{0}:{1}".format(jdbcHostname, jdbcPort)
 
connectionProperties = {   
    "accessToken": access_token.get('accessToken'),  
    "database": jdbcDatabase,  
    "trustServerCertificate" : "false", 
    "encrypt" : "true",  
    "driver" : "com.microsoft.sqlserver.jdbc.SQLServerDriver",  
    "hostNameInCertificate" : "*.database.windows.net" }

pushdown_query = """(
    SELECT * FROM testingdb.dbo.Wildfires
    ) t"""

# pushdown_query = spark.sql(f"SELECT * FROM {table_name}")

# pushdown_query = spark.sql("SELECT * FROM testingdb.dbo.Wildfires")

# pushdown_query2 = """(
#     SELECT Name = TABLE_SCHEMA + '.' + TABLE_NAME 
#     FROM INFORMATION_SCHEMA.TABLES
#     ) a"""

# SELECT * INTO testingdb.dbo.Wildfires_Backup
# FROM testingdb.dbo.Wildfires

# pushdown_query = """(
# CREATE EXTERNAL TABLE dbo.Wildfires_Backup (
# 	[YEAR] nvarchar(4000),
# 	[FIRES] nvarchar(4000),
# 	[TOTAL_HA] nvarchar(4000),
# 	[MAX_SIZE_HA] nvarchar(4000),
# 	[FIRES >200ha] nvarchar(4000),
# 	[TOTAL_HA (>200ha)] nvarchar(4000)
# 	)
# 	WITH (
# 	LOCATION = 'CrisHudici/Canada - wildland fire summary stats.csv',
# 	DATA_SOURCE = [dev-sandbox_stndlincaesa_dfs_core_windows_net],
# 	FILE_FORMAT = [SynapseDelimitedTextFormat]
# 	)
# GO

# SELECT TOP 100 * FROM dbo.Wildfires_Backup
# GO  ) t"""

df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

# df.write.jdbc(url=jdbcUrl, table= "Wildfires_Backup",properties=connectionProperties, mode="overwrite")

# df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query2, properties=connectionProperties)
# display(df)


# COMMAND ----------

