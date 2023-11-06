from flask import Flask, jsonify, g
from threading import Lock
import time
from datetime import datetime, timedelta
from logic import filter_schema_transactions, filter_schema_by_products, filter_schema_by_city

app = Flask(__name__)
lock = Lock()

@app.route("/assignment/transactions/<int:parameter>", methods=['GET'])
def get_transaction(parameter):
    with lock:
        time.sleep(2)
        #g.currDatetime = datetime.now()
        #currDatetime = g.currDatetime

        # For demonstration purposes, we will set the currDatetime as 2018-10-01 10:10:10, to align with our sample schema.
        g.currDatetime = datetime(2018, 10, 1, 10, 10, 10)
        currDatetime = g.currDatetime

        filtered_data = filter_schema_transactions(currDatetime, parameter)

        if not filtered_data:
            return jsonify({'error': 'Transaction not found'}), 404

        response_data = {
            'transactionId': filtered_data[0]['transactionId'],
            'productName': filtered_data[0]['productName'],
            'transactionAmount': filtered_data[0]['transactionAmount'],
            'transactionDatetime': filtered_data[0]['transactionDatetime']
        }

    return jsonify(response_data)


@app.route("/assignment/transactionSummaryByProducts/<int:parameter>", methods=['GET'])
def get_transaction_summary_by_product(parameter):
    with lock:
        time.sleep(2)
        # g.currDatetime = datetime.now()
        # currDatetime = g.currDatetime

        # For demonstration purposes, we will set the currDatetime as 2018-10-01 10:10:10, to align with our sample schema.
        g.currDatetime = datetime(2018, 10, 1, 10, 10, 10)
        currDatetime = g.currDatetime

        transaction_span_datetime = currDatetime - timedelta(days=parameter)

        transactions_by_product = filter_schema_by_products(currDatetime,transaction_span_datetime)
        response_data = {"summary": transactions_by_product}

    return jsonify(response_data)


@app.route("/assignment/transactionSummaryByManufacturingCity/<int:parameter>", methods=['GET'])
def get_transaction_summary_by_city(parameter):
    with lock:
        # g.currDatetime = datetime.now()
        # currDatetime = g.currDatetime

        # For demonstration purposes, we will set the currDatetime as 2018-10-01 10:10:10, to align with our sample schema.
        g.currDatetime = datetime(2018, 10, 1, 10, 10, 10)
        currDatetime = g.currDatetime

        transaction_span_datetime = currDatetime - timedelta(days=parameter)

        transactions_by_city = filter_schema_by_city(currDatetime,transaction_span_datetime)
        response_data = {"summary": transactions_by_city}

    return jsonify(response_data)


if __name__ == '__main__':
    app.run(debug=True, port=8080, host='0.0.0.0')
