import pandas as pd
from db import generate_schema_transactions, generate_schema_by_product

def filter_schema_transactions(currDatetime, parameter):

    curr_transact_csv, prod_reference_csv = generate_schema_transactions(currDatetime)

    try:
        key_column = 'productId'

        merged_transactions = pd.merge(curr_transact_csv,
                                       prod_reference_csv[['productId',
                                                           'productName']],
                                       on=key_column,
                                       how='left')

        merged_transactions.drop(columns=['productId'], inplace=True)

        merged_transactions = merged_transactions[['transactionId',
                                                   'productName',
                                                   'transactionAmount',
                                                   'transactionDatetime']]

        transactions = merged_transactions.to_dict(orient='records')

        filtered_data = []

        for item in transactions:
            if parameter == item['transactionId']:
                filtered_data.append(item)

        return filtered_data

    except FileNotFoundError:
        print(f"Error: File not found.")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def filter_schema_by_products(currDatetime,transaction_span_datetime):

    _, prod_reference_csv = generate_schema_transactions(currDatetime)
    curr_transact_csv = generate_schema_by_product(currDatetime,transaction_span_datetime)

    try:
        key_column = 'productId'

        merged_transactions = pd.merge(curr_transact_csv,
                                       prod_reference_csv[['productId',
                                                           'productName']],
                                       on=key_column,
                                       how='left')

        merged_transactions.drop(columns=['productId'], inplace=True)

        merged_transactions = merged_transactions[['transactionId',
                                                   'productName',
                                                   'transactionAmount',
                                                   'transactionDatetime']]

        total_transaction_amt = merged_transactions.groupby('productName')['transactionAmount'].sum().reset_index()

        transactions = total_transaction_amt.to_dict(orient='records')

        return transactions

    except FileNotFoundError:
        print(f"Error: File not found.")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None


def filter_schema_by_city(currDatetime, transaction_span_datetime):
    try:
        _, prod_reference_csv = generate_schema_transactions(currDatetime)
        curr_transact_csv = generate_schema_by_product(currDatetime, transaction_span_datetime)
        key_column = 'productId'

        merged_transactions = pd.merge(curr_transact_csv,
                                       prod_reference_csv[['productId',
                                                           'productName']],
                                       on=key_column,
                                       how='left')

        merged_transactions.drop(columns=['productId'], inplace=True)

        merged_transactions = merged_transactions[['transactionId', 'productName', 'transactionAmount', 'transactionDatetime']]

        total_transaction_amt = merged_transactions.groupby('productName')['transactionAmount'].sum().reset_index()

        key_column = 'productName'
        total_transactions_amt_by_city = pd.merge(total_transaction_amt,
                                                  prod_reference_csv[['productName',
                                                                      'productManufacturingCity']],
                                                  on=key_column, how='left')

        total_transactions_amt_by_city = total_transactions_amt_by_city.groupby('productManufacturingCity')['transactionAmount'].sum().reset_index()

        transactions = total_transactions_amt_by_city.to_dict(orient='records')

        return transactions

    except FileNotFoundError:
        print(f"Error: File not found.")
        return None

    except Exception as e:
        print(f"An error occurred: {e}")
        return None