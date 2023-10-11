import mysql.connector
from mysql.connector import Error
import pandas as pd
import wc_credential
import matplotlib.pyplot as plt

# Establish a connection to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user= wc_credential.mysql_username,
    password=wc_credential.mysql_password,
    database='creditcard_capstone'
)

# Function to fetch data from database and create DataFrame
def fetch_data_and_create_df(sql_query):
    cursor = conn.cursor()
    cursor.execute(sql_query)
    data_df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
    cursor.close()
    return data_df

# REQ 5.1: Find and plot the percentage of applications approved for self-employed applicants.
loan_data_df = fetch_data_and_create_df("SELECT * FROM cdw_sapp_loan_application;")
self_employed_df = loan_data_df[loan_data_df['Self_Employed'] == 'Yes']
percentage_approved = (self_employed_df['Application_Status'].value_counts(normalize=True) * 100)['Y']

labels = ['Approved', 'Not Approved']
sizes = [percentage_approved, 100 - percentage_approved]
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140, labeldistance=0.85)
plt.title('Percentage of Applications Approved for Self-Employed Applicants')
plt.axis('equal')
plt.show()

# REQ 5.2: Find the percentage of rejection for married male applicants.
married_male_rejected = loan_data_df[(loan_data_df['Married'] == 'Yes') & (loan_data_df['Gender'] == 'Male') & (loan_data_df['Application_Status'] == 'N')]
percentage_rejected = (len(married_male_rejected) / len(loan_data_df[(loan_data_df['Married'] == 'Yes') & (loan_data_df['Gender'] == 'Male')])) * 100

labels = ['Rejected', 'Approved']
sizes = [percentage_rejected, 100 - percentage_rejected]
plt.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=140)
plt.title('Percentage of Rejections for Married Male Applicants')
plt.axis('equal')
plt.show()

# REQ 5.3: Find and plot the top three months with the largest volume of transaction data.
cdw_sapp_credit_card_df = fetch_data_and_create_df("SELECT * FROM cdw_sapp_credit_card;")
cdw_sapp_credit_card_df['Transaction_Year'] = cdw_sapp_credit_card_df['TIMEID'].str[:4]
cdw_sapp_credit_card_df['Transaction_Month'] = cdw_sapp_credit_card_df['TIMEID'].str[4:6]
transaction_volume_by_month = cdw_sapp_credit_card_df.groupby(['Transaction_Year', 'Transaction_Month'])['TRANSACTION_VALUE'].sum()
top_three_months = transaction_volume_by_month.sort_values(ascending=False).head(3)

plt.figure(figsize=(10, 6))
ax = top_three_months.plot(kind='bar', color='skyblue')
plt.ylim(top_three_months.min() - 500, top_three_months.max() + 1000)
plt.xlabel('Month')
plt.ylabel('Transaction Volume')
plt.title('Top Three Months with the Largest Transaction Volume')

for p in ax.patches:
    ax.annotate(f'{p.get_height():,.0f}', (p.get_x() + p.get_width() / 2., p.get_height()), ha='center', va='bottom', fontsize=8, color='black', xytext=(0, 5), textcoords='offset points')

plt.xticks(rotation=0)
plt.tight_layout()
plt.show()

# REQ 5.4: Find and plot which branch processed the highest total dollar value of healthcare transactions.
healthcare_transactions = cdw_sapp_credit_card_df[cdw_sapp_credit_card_df['TRANSACTION_TYPE'] == 'Healthcare']
healthcare_by_branch = healthcare_transactions.groupby('BRANCH_CODE')['TRANSACTION_VALUE'].sum()
sorted_healthcare_by_branch = healthcare_by_branch.sort_values(ascending=False).head(10)

plt.figure(figsize=(12, 6))
sorted_healthcare_by_branch.plot(kind='bar', color='skyblue')
plt.ylim(sorted_healthcare_by_branch.min() - 1000, sorted_healthcare_by_branch.max() + 1000)
plt.xlabel('Branch Code')
plt.ylabel('Total Transaction Value')
plt.title('Total Transaction Value for Healthcare by Branch')
plt.xticks(rotation=45, fontsize=8)

highest_value_branch = sorted_healthcare_by_branch.idxmax()
highest_value = sorted_healthcare_by_branch.max()

plt.show()

print("Branch with the highest healthcare transaction value:", highest_value_branch)
print("Total transaction value for the highest branch:", highest_value)