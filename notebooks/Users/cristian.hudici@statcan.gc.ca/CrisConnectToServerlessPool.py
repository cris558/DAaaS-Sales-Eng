# Databricks notebook source
import adal
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
    SELECT Name = TABLE_SCHEMA + '.' + TABLE_NAME
    FROM INFORMATION_SCHEMA.TABLES
  ) t"""

pushdown_query2 = """(
    SELECT * FROM testingdb.dbo.Wildfires
  ) t"""

# pushdown_query = """(
#     SELECT * INTO testingdb.dbo.Wildfires_Backup
#     FROM testingdb.dbo.Wildfires
#     WHERE 1= 0
#   ) t"""

df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)

df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query2, properties=connectionProperties)
display(df)


# COMMAND ----------

