Assignment Overview:

In this coding assignment, an application that processes incoming CSV files in a streaming fashion from a designated folder is developed. The goal is to provide an up-to-date view of transactions for three (3) specific REST calls.

Assumptions:

1. CSV File Format:
    - Transaction information arrives as CSV files named 'Transaction_<datetime>.csv' in a folder named 'Transaction'.
    - Any files with different filenames or files from other folders are ignored.
    - Existing CSV files in the 'Transaction' folder will not be updated. Instead, new CSV files with updated datetime will be added to the folder.

2. Product Reference Data:
    - Product reference data is stored in a file named 'ProductReference.csv' within a folder named 'Reference'.

3. REST API Endpoint Modifications:
    - For the following API endpoints:
        http://localhost:8080/assignment/transactionSummaryByProducts/{last_n_days}
        http://localhost:8080/assignment/transactionSummaryByManufacturingCity/{last_n_days}
      Instead of receiving the summary of transactions from the last 10 days, the calls will receive the summary of transactions         from the last n days, where n = {last_n_days}, and n is a positive integer. The value of {last_n_days} will be provided as         input on the client side.
    
4. Date Format Consistency:
    - The 'transactionDatetime' in each transaction information and the <datetime> in 'Transaction_<datetime>.csv' follow the same       format: %Y%m%d%H%M%S.
      Note: With this assumption, 'Transaction_20180101101010.csv' is renamed to 'Transaction_20181001101010.csv' (change in             month).

Key Design Concepts:

1. Flask 'g' for Efficient Handling:
    - Flask's 'g' is utilized to handle concurrent requests efficiently. Each request has its unique 'currDatetime' stored in 'g'.

2. Locking to Prevent Repeated Requests:
    - Lock() is used so each API request is not repeated after being called.

3. Scalability through Separation of Concerns:
    - The application is structured with a clear separation of functions, handling database interactions, business logic, and API       endpoints. This design approach enhances the application's scalability.

Points for Improvement:

1. Big data transactions
    - Usual Flask applications utilize frontend websites or dedicated tools in handling big CSV data. The application can be             improved in this aspect by employing database-specific processes such as batch processing, indexing, among the others.

2. Scalability
    - The application can still be improved in its scalable design by introducing load balancers, asynchronous programming, etc.
