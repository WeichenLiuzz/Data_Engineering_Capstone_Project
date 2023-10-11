# REQ1.1 Data Extraction and Transformation with Python and PySpark
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark import SparkContext as sc
# Import random module for generating random area codes
import random
import wc_credential

# Initialize Spark session
spark = SparkSession.builder.appName("CreditCardSystemETL").getOrCreate()

# Set log level to Error
spark.sparkContext.setLogLevel("ERROR")

# Load CDW_SAPP_BRANCH.JSON into a DataFrame
cdw_sapp_branch_source_df = spark.read.json("C:/Users/vicky/Documents/perscholars/capstone/dataset/cdw_sapp_branch.json")

# Transformation based on mapping document
cdw_sapp_branch = cdw_sapp_branch_source_df \
    .withColumn("BRANCH_CODE", col("BRANCH_CODE").cast("int")) \
    .withColumn("BRANCH_NAME", col("BRANCH_NAME").cast("varchar(64)")) \
    .withColumn("BRANCH_STREET", col("BRANCH_STREET").cast("varchar(64)")) \
    .withColumn("BRANCH_CITY", col("BRANCH_CITY").cast("varchar(64)")) \
    .withColumn("BRANCH_STATE", col("BRANCH_STATE").cast("varchar(64)")) \
    .withColumn("BRANCH_ZIP", 
                when(col("BRANCH_ZIP").isNotNull(),
                     col("BRANCH_ZIP").cast("int"))
                .otherwise(99999)) \
    .withColumn("BRANCH_PHONE", 
                concat(lit("("), substring("BRANCH_PHONE", 1, 3), lit(")"), 
                        substring("BRANCH_PHONE", 4, 3), lit("-"), 
                        substring("BRANCH_PHONE", 7, 4)).cast("varchar(64)")) \
    .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast("timestamp"))


cdw_sapp_branch.printSchema()
cdw_sapp_branch.show(2)


# Load CDW_SAPP_CREDITCARD.JSON into a DataFrame
cdw_sapp_credit_source_df = spark.read.json("C:/Users/vicky/Documents/perscholars/capstone/dataset/cdw_sapp_credit.json")


# Transformation based on the mapping document
cdw_sapp_credit_card = cdw_sapp_credit_source_df \
    .withColumn("CUST_CC_NO", col("CREDIT_CARD_NO").cast("varchar(64)")) \
    .withColumn("MONTH", lpad(col("MONTH"), 2, '0')) \
    .withColumn("DAY", lpad(col("DAY"), 2, '0')) \
    .withColumn("TIMEID", concat(col("YEAR"), col("MONTH"), col("DAY")).cast("varchar(64)")) \
    .withColumn("CUST_SSN", col("CUST_SSN").cast("int")) \
    .withColumn("BRANCH_CODE", col("BRANCH_CODE").cast("int")) \
    .withColumn("TRANSACTION_TYPE", col("TRANSACTION_TYPE").cast("varchar(64)")) \
    .withColumn("TRANSACTION_VALUE", col("TRANSACTION_VALUE").cast("double")) \
    .withColumn("TRANSACTION_ID", col("TRANSACTION_ID").cast("int")) \
    .drop("DAY", "MONTH", "YEAR", "CUST_CC_NO") #do not need to extract them as separate columns
    

cdw_sapp_credit_card.printSchema()
cdw_sapp_credit_card.show(2)


# Load CDW_SAPP_CUSTOMER.JSON into a DataFrame
cdw_sapp_custmer_source_df = spark.read.json("C:/Users/vicky/Documents/perscholars/capstone/dataset/cdw_sapp_custmer.json")



# Define Random Area Codes
random_area_codes = ['123', '456', '789']

# Function to generate a random area code
def generate_random_area_code():
    return random.choice(random_area_codes)


# Register the function as a UDF (User Defined Function)
spark.udf.register("random_area_code", generate_random_area_code)


# Transformation based on the mapping document
cdw_sapp_custmer = cdw_sapp_custmer_source_df \
    .withColumn("SSN", col("SSN").cast("int")) \
    .withColumn("FIRST_NAME", initcap(col("FIRST_NAME")).cast("varchar(64)")) \
    .withColumn("MIDDLE_NAME", lower(col("MIDDLE_NAME")).cast("varchar(64)")) \
    .withColumn("LAST_NAME", initcap(col("LAST_NAME")).cast("varchar(64)")) \
    .withColumn("CREDIT_CARD_NO", col("CREDIT_CARD_NO").cast("varchar(64)")) \
    .withColumn("FULL_STREET_ADDRESS", concat(col("APT_NO"), lit(", "), col("STREET_NAME")).cast("varchar(64)")) \
    .withColumn("CUST_CITY", col("CUST_CITY").cast("varchar(64)")) \
    .withColumn("CUST_STATE", col("CUST_STATE").cast("varchar(64)")) \
    .withColumn("CUST_COUNTRY", col("CUST_COUNTRY").cast("varchar(64)")) \
    .withColumn("CUST_ZIP", col("CUST_ZIP").cast("int")) \
    .withColumn("RANDOM_AREA_CODE", expr("random_area_code()")) \
    .withColumn("CUST_PHONE", 
                concat(lit("("), 
                       col("RANDOM_AREA_CODE"), lit(")"), 
                       substring("CUST_PHONE", 1, 3), 
                       lit("-"), 
                       substring("CUST_PHONE", 4, 4)).cast("varchar(64)")) \
    .withColumn("CUST_EMAIL", col("CUST_EMAIL").cast("varchar(64)")) \
    .withColumn("LAST_UPDATED", col("LAST_UPDATED").cast("timestamp")) \
    .drop("STREET_NAME", "APT_NO", "RANDOM_AREA_CODE")




cdw_sapp_custmer.printSchema()
cdw_sapp_custmer.show(2)



# REQ1.2 Data loading into Database



cdw_sapp_branch.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_BRANCH") \
    .option("user", wc_credential.mysql_username) \
    .option("password", wc_credential.mysql_password) \
    .save()

cdw_sapp_credit_card.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_CREDIT_CARD") \
    .option("user",wc_credential.mysql_username) \
    .option("password", wc_credential.mysql_password) \
    .save()


cdw_sapp_custmer.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_CUSTOMER ") \
    .option("user", wc_credential.mysql_username) \
    .option("password", wc_credential.mysql_password) \
    .save()


