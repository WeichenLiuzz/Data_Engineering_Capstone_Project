# REQ 2.1 Transaction Details Module
import mysql.connector
import wc_credential
from mysql.connector import Error

conn = mysql.connector.connect(
    host='localhost',
    user= wc_credential.mysql_username,
    password=wc_credential.mysql_password,
    database='creditcard_capstone'
)

cursor = conn.cursor()

# Function to display transactions made by customers in a given zip code for a specific month and year


def display_transactions_by_zip(year, month, zip_code):
    query = f"""
    SELECT *
    FROM CDW_SAPP_CREDIT_CARD cc
    JOIN CDW_SAPP_BRANCH b ON cc.BRANCH_CODE = b.BRANCH_CODE
    WHERE SUBSTRING(cc.TIMEID, 1, 4) = '{year}'
      AND SUBSTRING(cc.TIMEID, 5, 2) = '{month}'
      AND b.BRANCH_ZIP = '{zip_code}'
    ORDER BY SUBSTRING(cc.TIMEID, 7, 2) DESC;
    """
    cursor.execute(query)
    transactions = cursor.fetchall()

    if not transactions:
        print(f"No transactions found for the given year, month, and zip code.")
    else:
        for transaction in transactions:
            print(transaction)





# Function to display the number and total values of transactions for a given type
def display_transactions_by_type(transaction_type):
    query = f"SELECT COUNT(*), SUM(TRANSACTION_VALUE) FROM CDW_SAPP_CREDIT_CARD WHERE TRANSACTION_TYPE = '{transaction_type}';"
    cursor.execute(query)
    result = cursor.fetchone()

    if result[0] == 0:
        print(f"No transactions found for the given transaction type: {transaction_type}")
    else:
        num_transactions = result[0]
        total_value = result[1]
        total_value_str = "{:.2f}".format(total_value)  # Convert to string with 2 decimal places
        print(f"Number of {transaction_type} transactions: {num_transactions}")
        print(f"Total value of {transaction_type} transactions: {total_value_str}")



# Function to display the total number and total values of transactions for branches in a given state
def display_transactions_by_state(state):
    query = f"""
    SELECT COUNT(cc.TRANSACTION_ID), SUM(cc.TRANSACTION_VALUE)
    FROM CDW_SAPP_CREDIT_CARD cc
    JOIN CDW_SAPP_BRANCH b ON cc.BRANCH_CODE = b.BRANCH_CODE
    WHERE b.BRANCH_STATE = '{state}';
    """
    cursor.execute(query)
    result = cursor.fetchone()

    if result[0] == 0:
        print(f"No transactions found for the given state: {state}")
    else:
        num_transactions = result[0]
        total_value = result[1]
        total_value_str = "{:.2f}".format(total_value)  # Convert to string with 2 decimal places
        print(f"Total number of transactions in {state}: {num_transactions}")
        print(f"Total value of transactions in {state}: {total_value_str}")



if __name__ == "__main__":
    while True:
        print("Select an option:")
        print("1. Display transactions by zip code for a given month and year.")
        print("2. Display number and total values of transactions for a given type.")
        print("3. Display total number and total values of transactions for branches in a given state.")
        print("---------------------------------------------------------------------------")

        try:
            option = input("Enter your choice (or press Enter to exit): ")

            if option.strip() == "":
                print("No option selected. Exiting...")
                break  # Break the loop and exit

            option = int(option)

            if option == 1:
                print("You chose: 1. Display transactions by zip code for a given month and year.")

                # Get a valid year (4 digits)
                while True:
                    year = input("Enter year (4 digits): ")
                    if year.isdigit() and len(year) == 4:
                        break
                    else:
                        print("Invalid year. Please enter a 4-digit year.")

                # Get a valid month (2 digits)
                while True:
                    month = input("Enter month (2 digits, e.g., 03 for March): ")
                    if month.isdigit() and len(month) == 2 and 1 <= int(month) <= 12:
                        # Ensure the month is in the valid range (1-12)
                        break
                    else:
                        print("Invalid month. Please enter a 2-digit month (e.g., 03 for March).")

                # Get a valid zip code
                zip_code = input("Enter zip code: ")
                display_transactions_by_zip(year, month, zip_code)


            elif option == 2:
                print("You chose: 2. Display number and total values of transactions for a given type.")
                transaction_types = """
                Available transaction types:
                Education
                Grocery
                Bills
                Entertainment
                Healthcare
                Gas
                Test
                """
                transaction_type = input(f"Enter transaction type ({transaction_types}): ")
                display_transactions_by_type(transaction_type)

            elif option == 3:
                print("You chose: 3. Display total number and total values of transactions for branches in a given state.")
                state_instruction = "To enter a state, use its abbreviation (e.g., NY for New York)."
                
                state = input(f"{state_instruction} Enter state abbreviation: ")
                display_transactions_by_state(state)
                
            else:
                print("Invalid option.")
        
        except ValueError:
            print("Invalid input. Please enter a valid option number.")
            
        # Close the database connection
    conn.close()


# REQ 2.2 Customer Details Module

# Establish a connection to the MySQL database
conn = mysql.connector.connect(
    host='localhost',
    user= wc_credential.mysql_username,
    password=wc_credential.mysql_password,
    database='creditcard_capstone'
)

cursor = conn.cursor()


# Function to check existing account details of a customer
def check_customer_account_details(first_name, middle_name, last_name):
    query = f"""
    SELECT *
    FROM CDW_SAPP_CUSTOMER
    WHERE FIRST_NAME = '{first_name}' AND MIDDLE_NAME = '{middle_name}' AND LAST_NAME = '{last_name}';
    """

    cursor.execute(query)
    results = cursor.fetchall()
    

    if len(results) == 0:
        print("No customer found for the provided name.")
    elif len(results) == 1:
        print_customer_details(results[0])
    else:
        print("Multiple customers found for the provided name.")

        # Ask for the last four digits of SSN for further identification
        ssn_last_four = input("Enter the last four digits of SSN: ")
        matching_customers = [result for result in results if result[11][-4:] == ssn_last_four]
        
        if len(matching_customers) == 1:
            print_customer_details(matching_customers[0])


# Function to print customer details
def print_customer_details(customer):
    print("Customer Details: ")
    print(f"""Credit Card Number: {customer[0]}, City: {customer[1]}, Country: {customer[2]}, 
          Email: {customer[3]}, Phone: {customer[4]}, State: {customer[5]},
          Zip Code: {customer[6]}, First Name: {customer[7]}, Last Name: {customer[8]}, Last Updated: {customer[9]},
          Middle Name: {customer[10]}, SSN: {customer[11]}, Full Street Address: {customer[12]}""")
    


# Function to modify existing account details of a customer
def modify_customer_account_details(cust_ssn):

    # Check if the provided SSN exists
    query_check_ssn = f"SELECT * FROM CDW_SAPP_CUSTOMER WHERE SSN = {cust_ssn};"
    cursor.execute(query_check_ssn)
    result = cursor.fetchone()

    if result is None:
        print("No customer found for the provided SSN.")
        return

    while True:
        print("""Available columns to modify: 1. Credit Card Number 2. City 3. Country 4. Email 5. Phone 6. State 7. Zip Code
              8. First Name 9. Last Name 10. Last Updated 11. Middle Name 12. SSN 13. Full Street Address""")
       
        # Map the column number to the corresponding column name and description
        column_info = {
            1: ("CREDIT_CARD_NO", "credit card number"),
            2: ("CUST_CITY", "city"),
            3: ("CUST_COUNTRY", "country"),
            4: ("CUST_EMAIL", "email"),
            5: ("CUST_PHONE", "phone number (format: (XXX)-XXX-XXXX)"),
            6: ("CUST_STATE", "state (use its abbreviation e.g., NY for New York)"),
            7: ("CUST_ZIP", "zip code"),
            8: ("FIRST_NAME", "first name (use Title Case e.g., Tom for TOM)"),
            9: ("LAST_NAME", "last name (use Title Case e.g., Smith for SMITH)"),
            10: ("LAST_UPDATED", "last updated (use TIMESTAMP format e.g., 2018-04-21 09:49:02)"),
            11: ("MIDDLE_NAME", "middle name (use lower case e.g., james for JAMES)"),
            12: ("SSN", "SSN"),
            13: ("FULL_STREET_ADDRESS", "full street address (Apartment no and Street name of customer's Residence)")
        }

        column_number = int(input("Enter the number of the column you want to modify: "))
        column_info = column_info.get(column_number)

        if column_info:
            column_name, column_description = column_info
            new_value = input(f"Enter new value for {column_description}: ")

            # Construct the query to update the customer details
            set_value = f"{column_name} = '{new_value}'"
            query = f"UPDATE CDW_SAPP_CUSTOMER SET {set_value} WHERE SSN = {cust_ssn};"

            # Execute the query and commit the changes
            cursor.execute(query)
            conn.commit()

            print(f"Updated {column_description} for SSN {cust_ssn} to: {new_value}")
        else:
            print("Invalid column number.")

        confirm = input("Do you want to proceed with more changes? (yes/no): ")

        if confirm.lower() == "no":
            print("Customer account details updated successfully.")
            break

# Function to generate a monthly bill for a credit card number for a given month and year
def generate_monthly_bill(credit_card_no, year, month):
    query = f"""
    SELECT *
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CREDIT_CARD_NO = '{credit_card_no}' 
        AND SUBSTRING(TIMEID, 1, 4) = '{year}'
        AND SUBSTRING(TIMEID, 5, 2) = '{month}'
    ORDER BY SUBSTRING(TIMEID, 7, 2) DESC;
    """
    cursor.execute(query)
    transactions = cursor.fetchall()

    if not transactions:
        print("No transactions found for the provided credit card number, year, and month.")
        return
    

    # Initialize total_amount
    total_amount = 0
    

    
    print("Monthly Bill:")
    print(f"Credit Card Number: {credit_card_no}")
    print(f"Year: {year}")
    print(f"Month: {month}")
    print("Transaction Details:")
    for transaction in transactions:
        print(f"Credit Card No: {transaction[1]}")
        print(f"Customer SSN: {transaction[2]}")
        print(f"Transaction ID: {transaction[3]}")
        print(f"Transaction Type: {transaction[4]}")
        print(f"Transaction Value: {transaction[5]}")
        print(f"Transaction Date: {transaction[6]}")
        print(f"Branch Code: {transaction[0]}")
        print("-----------------------------------")

        # Accumulate transaction value to total_amount
        total_amount += float(transaction[5])
    total_amount_str = "{:.2f}".format(total_amount)

    print(f"Total Amount: {total_amount_str}")


# Function to display transactions made by a customer between two dates
def display_transactions_by_customer_and_dates(cust_ssn, start_date, end_date):
    query = f"""
    SELECT *
    FROM CDW_SAPP_CREDIT_CARD
    WHERE CUST_SSN = {cust_ssn} 
        AND TIMEID BETWEEN '{start_date}' AND '{end_date}'
    ORDER BY SUBSTRING(TIMEID, 1, 4), SUBSTRING(TIMEID, 5, 2), SUBSTRING(TIMEID, 7, 2) DESC;
    """
    cursor.execute(query)
    transactions = cursor.fetchall()

    if not transactions:
        print("No transactions found for the provided Customer SSN, start_date, and end_date.")
        return
    
    print(f"Transactions for Customer SSN {cust_ssn} between {start_date} and {end_date}:")
    for transaction in transactions:
        print(f"Credit Card No: {transaction[1]}")
        print(f"Customer SSN: {transaction[2]}")
        print(f"Transaction ID: {transaction[3]}")
        print(f"Transaction Type: {transaction[4]}")
        print(f"Transaction Value: {transaction[5]}")
        print(f"Transaction Date: {transaction[6]}")
        print(f"Branch Code: {transaction[0]}")
        print("---------------------------------------------------------------------------")

if __name__ == "__main__":
    while True:
        print("Select an option:")
        print("1. Check existing account details of a customer.")
        print("2. Modify existing account details of a customer.")
        print("3. Generate a monthly bill for a credit card number.")
        print("4. Display transactions made by a customer between two dates.")
        print("---------------------------------------------------------------------------")

        try:
            option = input("Enter your choice (or press Enter to exit): ")

            if option.strip() == "":
                print("No option selected. Exiting...")
                break  # Break the loop and exit

            option = int(option)

            if option == 1:
                print("You chose: 1. Check existing account details of a customer.")
                first_name = input("Enter customer first name: ")
                middle_name = input("Enter customer middle_name: ")
                last_name = input("Enter customer last name: ")
                check_customer_account_details(first_name, middle_name, last_name)
        
            elif option == 2:
                print("You chose: 2. Modify existing account details of a customer.")
                cust_ssn = int(input("Enter customer SSN: "))
                modify_customer_account_details(cust_ssn)
                
            elif option == 3:
                credit_card_no = input("Enter credit card number: ")
                year = input("Enter year (YYYY): ")
                month = input("Enter month (MM): ")
                generate_monthly_bill(credit_card_no, year, month)
            elif option == 4:
                cust_ssn = int(input("Enter customer SSN: "))
                start_date = input("Enter start date ((YYYYMMDD)): ")
                end_date = input("Enter end date ((YYYYMMDD)): ")
                display_transactions_by_customer_and_dates(cust_ssn, start_date, end_date)
            else:
                print("Invalid option.")
        
        except ValueError:
            print("Invalid input. Please enter a valid option number.")

