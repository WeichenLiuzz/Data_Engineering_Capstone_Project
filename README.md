# CAP 350 - Data Engineering Capstone Project

## Table of Contents
* **Scope of Works**
* **Project Structure**
    - `credit_card_etl.py`
    - `console_based_program.py`
    - `data_analysis_and_visualization.py`
    - `loan_application_etl.py`
    - `data_analysis_and_visualization_LOAN.py`
    - `requirements.txt`
    - `README.md`
* **Data Sources & Description**
* **Workflow Diagram**
* **Conclusion**


## Scope of Works
This repository contains the capstone project for the CAP 350 - Data Engineering course, focusing on ETL processes for a Loan Application dataset and a Credit Card dataset using various technologies such as Python (Pandas, advanced modules, Matplotlib), SQL, and Apache Spark (Spark Core, Spark SQL), along with Python Visualization and Analytics libraries.


## Project Structure
* **`credit_card_etl.py`:** Python script for ETL (Extraction, Transformation, Loading) process for Credit Card System. The ETL process involves extracting data from specified JSON files, transforming the data based on mapping document requirements, and loading it into a MySQL database.

* **`console_based_program.py`:** Console-based Python program for displaying transaction and customer details. This module fulfills various functional requirements related to transaction and customer details display.

    The Transaction Details Module fulfills the following functional requirements:
    - Display transactions made by customers residing in a specified zip code for a given month and year. The display should be ordered by day in descending order.
    - Display the number and total values of transactions for a specific transaction type.
    - Display the overall number and total values of transactions for branches located in a particular state.

    The Customer Details Module meets the following functional requirements:
    - Check the existing account details of a customer.
    - Modify the existing account details of a customer.
    - Generate a monthly bill for a credit card number for a specified month and year.
    - Display the transactions made by a customer within a defined date range, ordering by year, month, and day in descending order.

* **`data_analysis_and_visualization.py`:** Python script for data analysis and visualization for the credit card system. It includes three visualizations:
    - Analyzing Transaction Types with Highest Transaction Count
    - Identifying States with a High Customer Count
    - Summing Transactions for the Top 10 Customers

* **`loan_application_etl.py`:** Python script for ETL process for Loan Application dataset. This part includes creating a Python program to retrieve data from a specified API endpoint, checking the status code of the API, and utilizing PySpark to load this data into an RDBMS table named `CDW_SAPP_loan_application` within the `creditcard_capstone` database.

* **`data_analysis_and_visualization_LOAN.py`:** Python script for data analysis and visualization for LOAN Application, including four visualizations.

    **Percentage of Applications Approved for Self-Employed Applicants:**
    The task involves analyzing the loan application data to determine the percentage of approvals for self-employed applicants. This outcome is presented in a pie chart for clear visualization.

    **Percentage of Rejections for Married Male Applicants:** 
    This part identifies the percentage of rejections for married male applicants. The result is presented in a pie chart for a visual representation.

    **Top Three Months with Highest Transaction Volume:** 
    This requirement analyzes the top three months with the highest transaction volume. The data is grouped by month and year, and the top three months with the highest transaction volume is presented in a bar chart.

    **Branch with the Highest Healthcare Transaction Value:**
    The objective is to ascertain the branch that processed the highest total dollar value of healthcare transactions. The analysis involves aggregating transaction values by branch and identifying the branch with the maximum value. This information is visually conveyed using a bar chart.

* **`requirements.txt:`** List of required Python libraries for the project.
* **`README.md:`** Project documentation explaining details, development comments, technical challenges, and workflow diagrams.

## Data Sources & Description
* **CDW_SAPP_BRANCH.JSON:** This JSON file contains data related to branches of the Credit Card System. Fields include BRANCH_CITY, BRANCH_CODE, BRANCH_NAME, BRANCH_STATE, BRANCH_STREET, BRANCH_ZIP and LAST_UPDATED. This table name in `creditcard_capston` Database is `CDW_SAPP_BRANCH`.

* **CDW_SAPP_CREDITCARD.JSON:** This JSON file contains data related to credit card transactions within the Credit Card System. Fields include BRANCH_CODE, CREDIT_CARD_NO, CUST_SSN, TRANSACTION_ID, TRANSACTION_TYPE, TRANSACTION_VALUE and TIMEID. This table name in `creditcard_capston` Database is `CDW_SAPP_CREDIT_CARD`.

* **CDW_SAPP_CUSTOMER.JSON:** This JSON file contains data related to customers of the Credit Card System. Fields include CREDIT_CARD_NO, CUST_CITY, CUST_COUNTRY, CUST_EMAIL, CUST_PHONE, CUST_STATE, CUST_ZIP, FIRST_NAME, LAST_NAME, LAST_UPDATED, MIDDLE_NAME, SSN and FULL_STREET_ADDRESS. This table name in `creditcard_capston` Database is `CDW_SAPP_CUSTOMER`.

* **CDW_SAPP_LOAN_APPLICATION:** This data file contains data related to loan applications. Fields include Application_ID, Gender, Married, Dependents, Education, Self_Employed, Credit_History, Property_Area, Income and Application_Status. This table name in the `creditcard_capstone` Database is `CDW_SAPP_LOAN_APPLICATION`.

## Workflow Diagram
For a visual representation of the project workflow and application requirements, refer to the workflow diagram.<img src="schema.png"/>