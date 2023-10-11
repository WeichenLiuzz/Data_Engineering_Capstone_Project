# REQ 4.1 Create a Python program to GET (consume) data from the above API endpoint for the loan application dataset.
import requests
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import wc_credential

def get_loan_data(api_endpoint):
    response = requests.get(api_endpoint)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error accessing the API. Status code: {response.status_code}")
        return None

# API endpoint for loan data
api_endpoint = "https://raw.githubusercontent.com/platformps/LoanDataset/main/loan_data.json"

# Call the function to get the loan data
loan_data = get_loan_data(api_endpoint)

# Convert to a DataFrame for easy display
loan_data_df = pd.DataFrame(loan_data)

# Display the first few rows
loan_data_df.head()


# REQ 4.2 Find the status code of the above API endpoint.
def get_api_status_code(api_endpoint):
    response = requests.get(api_endpoint)
    return response.status_code

# Call the function to get the status code
status_code = get_api_status_code(api_endpoint)
print("Status code:", status_code)


# REQ 4.3 utilize PySpark to load data into RDBMS (SQL)
# Create a Spark session
spark = SparkSession.builder.appName("LoanApplicationLoader").getOrCreate()

# Convert the pandas DataFrame to a PySpark DataFrame
loan_data_spark = spark.createDataFrame(loan_data_df)
loan_data_spark.printSchema()
loan_data_spark.show(2)

# Write the DataFrame to the RDBMS table

loan_data_spark.write.format("jdbc") \
    .mode("overwrite") \
    .option("url", "jdbc:mysql://localhost:3306/creditcard_capstone") \
    .option("dbtable", "creditcard_capstone.CDW_SAPP_loan_application") \
    .option("user", wc_credential.mysql_username) \
    .option("password", wc_credential.mysql_password) \
    .save()
print("Data successfully written to the RDBMS table.")
