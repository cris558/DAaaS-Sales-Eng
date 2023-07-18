# Databricks notebook source
import adal # Active Directory Authentication Library
# use an existing keyvault to get the authentication credentials ("secrets")
keyvault = "devsandbox-kv1"
 
# get the secrets from the vault
tenant = dbutils.secrets.get(scope = keyvault, key = "TenantID")
login_url = "https://login.microsoftonline.com"
authority_url = (login_url + '/' + tenant)

context = adal.AuthenticationContext(authority_url, timeout=None)

# get an authentication token 
access_token = context.acquire_token_with_client_credentials(
    "https://database.windows.net/",
    dbutils.secrets.get(scope = keyvault, key = "DataBricksClientID"),
    dbutils.secrets.get(scope = keyvault, key = "DataBricksSecret"))

# connect to the serverless pool using jdbc
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

# send the query to the severless pool
pushdown_query = """(
    SELECT * FROM testingdb.dbo.Wildfirestest
    ) t"""



# COMMAND ----------

# fill the dataframe with the results of the query
df = spark.read.jdbc(url=jdbcUrl, table=pushdown_query, properties=connectionProperties)
display(df)
someRecords = df.where(df.TOTAL_HA.cast("int") > 1000000)
display(someRecords)


# COMMAND ----------

# someRecords = df.select('Year', 'Total_HA').where('TOTAL_HA' > '1,000,000')
df = df.where(df.TOTAL_HA.cast("int") > 100000)
display(df)

# COMMAND ----------

# import org.apache.spark.sql.functions.lit
# import org.apache.spark.sql.functions._

df = Seq(
  (2, true),
  (6, true),
  (5, false)
).toDF("num", "is_even_hardcoded")
display(df)
# def isEven(inputColName: String, outputColName: String)(df: DataFrame) = {
#   df.withColumn(outputColName, col(inputColName) % 2 === lit(0))
# }
# df.transform(isEven("num", "is_even")).show()

# COMMAND ----------

