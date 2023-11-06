from flask import Flask, jsonify
import pandas as pd
from datetime import datetime, timedelta
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

app = Flask(__name__)


@app.route("/assignment/transactions/<int:parameter>", methods=['GET'])
def get_transaction(parameter):
    # Get the current date and time
    #currDatetime = datetime.now()
    #For demonstration purposes, we will set the currDatetime as 2018-10-01 10:10:10, to align with our sample schema.
    currDatetime = datetime(2018, 10, 1, 10, 10, 10)

    #Extract current schema.
    transactions = generate_schema_transactions(currDatetime)

    #Filter schema based on input parameter.
    #Initialize list
    filtered_data = []

    for item in transactions:
        if parameter == item['transactionId']:
            filtered_data.append(item)

    if not filtered_data:
        return jsonify({'error': 'Transaction not found'}), 404

    #Here, we set [0] to retrieve the singular dictionary (or object), and not a list containing one dictionary (or object).
    response_data = {
        'transactionId': filtered_data[0]['transactionId'],
        'productName': filtered_data[0]['productName'],
        'transactionAmount': filtered_data[0]['transactionAmount'],
        'transactionDatetime': filtered_data[0]['transactionDatetime']
    }

    return jsonify(response_data)


@app.route("/assignment/transactions/<int:parameter>", methods=['GET'])
def generate_schema_transactions(currDatetime):

    # Format the current date and time as "Transaction_YYYYMMDDHHMMSS"
    curr_transaction_csv_name = currDatetime.strftime("Transaction_%Y%m%d%H%M%S") + ".csv"
    reference_csv = 'ProductReference.csv'
    transaction_csv_fold = '../pythonProject/Transaction/'
    relative_path_reference = '../pythonProject/Reference/'
    curr_transact_csv = pd.read_csv(f"{transaction_csv_fold}{curr_transaction_csv_name}")
    prod_reference_csv = pd.read_csv(f"{relative_path_reference}{reference_csv}")

    try:
        # 1.
        # We will first generate our first database, which is a left join of the transactions and product reference.
        # The common key is the productId, so we will assign the productName for each transaction.
        key_column = 'productId'

        # By using left join, we will replace the productId values in transactions with the productName values in product references.
        merged_transactions = pd.merge(curr_transact_csv,
                                       prod_reference_csv[['productId', 'productName']],
                                       on=key_column,
                                       how='left')

        # The left join will retain the productId column, so we will drop it without creating a new dataframe.
        merged_transactions.drop(columns=['productId'], inplace=True)

        # Columns will be rearranged per the desired output.
        merged_transactions = merged_transactions[['transactionId','productName','transactionAmount','transactionDatetime']]

        # Convert merged_transactions to a list of dictionaries
        transactions = merged_transactions.to_dict(orient='records')

        return transactions

    except FileNotFoundError:
        print(f"Error: File not found.")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


@app.route("/assignment/transactionSummaryByProducts/<int:parameter>", methods=['GET'])
def transaction_summary_by_product(parameter):
    # Get the current date and time
    # currDatetime = datetime.now()

    # For demonstration purposes, we will set the currDatetime as 2018-10-01 10:10:10, to align with our sample schema.
    currDatetime = datetime(2018, 10, 1, 10, 10, 10)
    transaction_span_datetime = currDatetime - timedelta(days=parameter)

    # Extract current schema.
    transactions_by_product = generate_schema_by_products(currDatetime,transaction_span_datetime)
    output_data = {"summary": transactions_by_product}
    return jsonify(output_data)


@app.route("/assignment/transactionSummaryByProducts/<int:parameter>", methods=['GET'])
def generate_schema_by_products(currDatetime,transaction_span_datetime):
    # Folder containing CSV files
    csv_folder = '../pythonProject/Transaction/'

    # List to store filtered dataframes
    filtered_dfs = []

    for file in os.listdir(csv_folder):
        if file.endswith('.csv') and file.startswith('Transaction_'):
            # Read CSV file into a DataFrame
            csv_path = os.path.join(csv_folder, file)
            df = pd.read_csv(csv_path)

            #Include same day CSV files.
            end_of_day = currDatetime.replace(hour=23, minute=59, second=59)

            # Convert 'transactionDatetime' column to datetime format
            df['transactionDatetime'] = pd.to_datetime(df['transactionDatetime'])

            # Filter rows based on transactionDatetime
            filtered_df = df[
                #(df['transactionDatetime'] <= currDatetime) &
                (df['transactionDatetime'] <= end_of_day) &
                (df['transactionDatetime'] >= transaction_span_datetime)
                ]

            # Add filtered DataFrame to the list
            filtered_dfs.append(filtered_df)

    # Concatenate all filtered dataframes into a single dataframe
    result_df = pd.concat(filtered_dfs, ignore_index=True)

    reference_csv = 'ProductReference.csv'
    relative_path_reference = '../pythonProject/Reference/'
    curr_transact_csv = result_df
    prod_reference_csv = pd.read_csv(f"{relative_path_reference}{reference_csv}")

    try:
        # 1.
        # We will first generate our first database, which is a left join of the transactions and product reference.
        # The common key is the productId, so we will assign the productName for each transaction.
        key_column = 'productId'

        # By using left join, we will replace the productId values in transactions with the productName values in product references.
        merged_transactions = pd.merge(curr_transact_csv,
                                       prod_reference_csv[['productId', 'productName']],
                                       on=key_column,
                                       how='left')

        # The left join will retain the productId column, so we will drop it without creating a new dataframe.
        merged_transactions.drop(columns=['productId'], inplace=True)

        # Columns will be rearranged per the desired output.
        merged_transactions = merged_transactions[['transactionId', 'productName', 'transactionAmount', 'transactionDatetime']]

        total_transaction_amt = merged_transactions.groupby('productName')['transactionAmount'].sum().reset_index()

        # Convert merged_transactions to a list of dictionaries
        transactions = total_transaction_amt.to_dict(orient='records')

        return transactions

    except FileNotFoundError:
        print(f"Error: File not found.")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


@app.route("/assignment/transactionSummaryByManufacturingCity/<int:parameter>", methods=['GET'])
def transaction_summary_by_city(parameter):
    # Get the current date and time
    # currDatetime = datetime.now()

    # For demonstration purposes, we will set the currDatetime as 2018-10-01 10:10:10, to align with our sample schema.
    currDatetime = datetime(2018, 10, 1, 10, 10, 10)
    transaction_span_datetime = currDatetime - timedelta(days=parameter)

    # Extract current schema.
    transactions_by_city = generate_schema_by_city(currDatetime,transaction_span_datetime)
    output_data = {"summary": transactions_by_city}
    return jsonify(output_data)


@app.route("/assignment/transactionSummaryByManufacturingCity/<int:parameter>", methods=['GET'])
def generate_schema_by_city(currDatetime,transaction_span_datetime):
    # Folder containing CSV files
    csv_folder = '../pythonProject/Transaction/'

    # List to store filtered dataframes
    filtered_dfs = []

    for file in os.listdir(csv_folder):
        if file.endswith('.csv') and file.startswith('Transaction_'):
            # Read CSV file into a DataFrame
            csv_path = os.path.join(csv_folder, file)
            df = pd.read_csv(csv_path)

            #Include same day CSV files.
            end_of_day = currDatetime.replace(hour=23, minute=59, second=59)

            # Convert 'transactionDatetime' column to datetime format
            df['transactionDatetime'] = pd.to_datetime(df['transactionDatetime'])

            # Filter rows based on transactionDatetime
            filtered_df = df[
                #(df['transactionDatetime'] <= currDatetime) &
                (df['transactionDatetime'] <= end_of_day) &
                (df['transactionDatetime'] >= transaction_span_datetime)
                ]

            # Add filtered DataFrame to the list
            filtered_dfs.append(filtered_df)

    # Concatenate all filtered dataframes into a single dataframe
    result_df = pd.concat(filtered_dfs, ignore_index=True)

    reference_csv = 'ProductReference.csv'
    relative_path_reference = '../pythonProject/Reference/'
    curr_transact_csv = result_df
    prod_reference_csv = pd.read_csv(f"{relative_path_reference}{reference_csv}")

    try:
        # 1.
        # We will first generate our first database, which is a left join of the transactions and product reference.
        # The common key is the productId, so we will assign the productName for each transaction.
        key_column = 'productId'

        # By using left join, we will replace the productId values in transactions with the productName values in product references.
        merged_transactions = pd.merge(curr_transact_csv,
                                       prod_reference_csv[['productId', 'productName']],
                                       on=key_column,
                                       how='left')

        # The left join will retain the productId column, so we will drop it without creating a new dataframe.
        merged_transactions.drop(columns=['productId'], inplace=True)

        # Columns will be rearranged per the desired output.
        merged_transactions = merged_transactions[['transactionId', 'productName', 'transactionAmount', 'transactionDatetime']]

        total_transaction_amt = merged_transactions.groupby('productName')['transactionAmount'].sum().reset_index()

        #We will also use the same logic in the previous item to generate the total sum of transactionAmount per city.
        #To do this, we will use left join to determine which productName values are produced from each productManufacturingCity
        key_column = 'productName'
        total_transactions_amt_by_city = pd.merge(total_transaction_amt,
                                                  prod_reference_csv[['productName', 'productManufacturingCity']],
                                                  on=key_column, how='left')

        total_transactions_amt_by_city = total_transactions_amt_by_city.groupby('productManufacturingCity')['transactionAmount'].sum().reset_index()

        # Convert merged_transactions to a list of dictionaries
        transactions = total_transactions_amt_by_city.to_dict(orient='records')

        return transactions

    except FileNotFoundError:
        print(f"Error: File not found.")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


if __name__ == '__main__':
    app.run(debug=True, port=8080, host='0.0.0.0')