import pandas as pd
import os

script_dir = os.path.dirname(os.path.abspath(__file__))
os.chdir(script_dir)

csv_folder = '../pythonProject/Transaction/'
relative_path_reference = '../pythonProject/Reference/'

def generate_schema_transactions(currDatetime):
    curr_transaction_csv_name = currDatetime.strftime("Transaction_%Y%m%d%H%M%S") + ".csv"
    reference_csv = 'ProductReference.csv'

    global csv_folder
    global relative_path_reference

    curr_transact_csv = pd.read_csv(f"{csv_folder}{curr_transaction_csv_name}")
    prod_reference_csv = pd.read_csv(f"{relative_path_reference}{reference_csv}")

    return curr_transact_csv, prod_reference_csv

def generate_schema_by_product(currDatetime,transaction_span_datetime):
    global csv_folder

    filtered_dfs = []

    for file in os.listdir(csv_folder):
        if file.endswith('.csv') and file.startswith('Transaction_'):
            csv_path = os.path.join(csv_folder, file)
            df = pd.read_csv(csv_path)

            end_of_day = currDatetime.replace(hour=23, minute=59, second=59)

            df['transactionDatetime'] = pd.to_datetime(df['transactionDatetime'])

            filtered_df = df[
                (df['transactionDatetime'] <= end_of_day) &
                (df['transactionDatetime'] >= transaction_span_datetime)
                ]

            filtered_dfs.append(filtered_df)

    result_df = pd.concat(filtered_dfs, ignore_index=True)

    curr_transact_csv = result_df
    return curr_transact_csv

def generate_schema_by_city(currDatetime,transaction_span_datetime):
    global csv_folder

    filtered_dfs = []

    for file in os.listdir(csv_folder):
        if file.endswith('.csv') and file.startswith('Transaction_'):
            csv_path = os.path.join(csv_folder, file)
            df = pd.read_csv(csv_path)

            end_of_day = currDatetime.replace(hour=23, minute=59, second=59)

            df['transactionDatetime'] = pd.to_datetime(df['transactionDatetime'])

            filtered_df = df[
                (df['transactionDatetime'] <= end_of_day) &
                (df['transactionDatetime'] >= transaction_span_datetime)
                ]

            filtered_dfs.append(filtered_df)

    result_df = pd.concat(filtered_dfs, ignore_index=True)

    curr_transact_csv = result_df
    return curr_transact_csv