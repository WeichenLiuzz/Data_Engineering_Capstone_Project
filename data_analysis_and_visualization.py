import mysql.connector
from mysql.connector import Error
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import wc_credential

def establish_db_connection():
    # Establish a connection to the MySQL database
    conn = mysql.connector.connect(
        host='localhost',
        user=wc_credential.mysql_username,
        password=wc_credential.mysql_password,
        database='creditcard_capstone'
    )
    return conn

def fetch_data_to_dataframe(cursor, sql_query):
    # Execute the SQL query
    cursor.execute(sql_query)
    # Fetch the data into a DataFrame
    df = pd.DataFrame(cursor.fetchall(), columns=cursor.column_names)
    return df


def plot_transaction_values(transaction_types, total_transaction_values):
    # Plot the data
    plt.figure(figsize=(12, 6))
    bars = plt.bar(transaction_types, total_transaction_values, color='skyblue')
    for bar, value in zip(bars, total_transaction_values):
        plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height(), f'{value:.2f}', ha='center', va='bottom', color='black')
    plt.ylim(min_total_transaction_value - 500, max_total_transaction_value + 500)
    plt.xlabel('Transaction Type')
    plt.ylabel('Total Transaction Value')
    plt.title('Total Transaction Value by Transaction Type')
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def plot_customer_count(customer_count_by_state_sorted):
    # Plot the data
    plt.figure(figsize=(12, 6))
    plt.bar(customer_count_by_state_sorted.index, customer_count_by_state_sorted.values, color='skyblue')
    plt.xlabel('State')
    plt.ylabel('Customer Count')
    plt.title('Customer Count by State')
    plt.xticks(rotation=45)
    plt.show()


def display_highest_customer_count_state(customer_count_by_state_sorted):
    highest_customer_count_state = customer_count_by_state_sorted.idxmax()
    highest_customer_count = customer_count_by_state_sorted.max()
    print(f"Highest customer count: {highest_customer_count} in state {highest_customer_count_state}")

# REQ 3.1 Find and plot which transaction type has the highest transaction count.
conn = establish_db_connection()
cursor = conn.cursor()  # Create a cursor
cdw_sapp_credit_card_df = fetch_data_to_dataframe(cursor, "SELECT * FROM cdw_sapp_credit_card;")
cursor.close()  # Close the cursor
conn.close()  # Close the database connection
#cdw_sapp_credit_card_df.head()

# Process transaction data
transaction_value_sum = cdw_sapp_credit_card_df.groupby('TRANSACTION_TYPE')['TRANSACTION_VALUE'].sum().reset_index()
transaction_value_sum = transaction_value_sum.rename(columns={'TRANSACTION_VALUE': 'Total_Transaction_Value'})
transaction_value_sum_sorted = transaction_value_sum.sort_values(by='Total_Transaction_Value', ascending=False)
transaction_types = transaction_value_sum_sorted['TRANSACTION_TYPE'].tolist()
total_transaction_values = transaction_value_sum_sorted['Total_Transaction_Value'].tolist()
min_total_transaction_value = np.min(total_transaction_values)
max_total_transaction_value = np.max(total_transaction_values)

# Plot transaction values
plot_transaction_values(transaction_types, total_transaction_values)

# Find and display the highest transaction value and type
index_of_max_transaction_value = np.argmax(total_transaction_values)
highest_transaction_type = transaction_types[index_of_max_transaction_value]
highest_transaction_value = total_transaction_values[index_of_max_transaction_value]
print(f"Highest transaction value: {highest_transaction_value:.2f} for transaction type {highest_transaction_type}")


# REQ 3.2 Find and plot which state has a high number of customers.
conn = establish_db_connection()
cursor = conn.cursor()  # Create a cursor
cdw_sapp_customer_df = fetch_data_to_dataframe(cursor, "SELECT * FROM cdw_sapp_customer;")
cursor.close()  # Close the cursor
conn.close()  # Close the database connection
#cdw_sapp_customer_df.head()

# Process customer data
customer_count_by_state = cdw_sapp_customer_df['CUST_STATE'].value_counts()
customer_count_by_state_sorted = customer_count_by_state.sort_values(ascending=False)

# Plot customer count by state
plot_customer_count(customer_count_by_state_sorted)
display_highest_customer_count_state(customer_count_by_state_sorted)

# REQ 3.3 Find and plot the sum of all transactions for the top 10 customers, and which customer has the highest transaction amount.
transaction_sum_by_customer = cdw_sapp_credit_card_df.groupby('CUST_SSN').agg({'TRANSACTION_VALUE': 'sum'}).reset_index()
transaction_sum_by_customer = transaction_sum_by_customer.rename(columns={'TRANSACTION_VALUE': 'Total_Transaction_Amount'})
transaction_sum_by_customer_sorted = transaction_sum_by_customer.sort_values(by='Total_Transaction_Amount', ascending=False)
top_10_customers = transaction_sum_by_customer_sorted.head(10)
#print(top_10_customers)

# Plot total transaction amount for top 10 customers
plt.figure(figsize=(12, 6))
bars = plt.bar(range(len(top_10_customers)), top_10_customers['Total_Transaction_Amount'], color='skyblue')

# Determine the minimum and maximum transaction amounts for Y-axis limits
min_transaction_amount = top_10_customers['Total_Transaction_Amount'].min()
max_transaction_amount = top_10_customers['Total_Transaction_Amount'].max()

# Set the Y-axis limits to start from the minimum transaction amount plus 100
plt.ylim(min_transaction_amount-100, max_transaction_amount+100)
plt.xlabel('Top 10 Customers')
plt.ylabel('Total Transaction Amount')
plt.title('Total Transaction Amount for Top 10 Customers')

# Annotate the bars with customer SSN
plt.xticks(range(len(top_10_customers)), [str(int(ssn)) for ssn in top_10_customers['CUST_SSN']], rotation=45)

plt.show()

# Display the highest transaction amount and the respective customer
customer_highest_transaction = top_10_customers.iloc[0]
print(f"Customer SSN: {int(customer_highest_transaction['CUST_SSN'])} with the highest transaction amount {customer_highest_transaction['Total_Transaction_Amount']:.2f}")
