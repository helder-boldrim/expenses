This project is designed for managing expenses. 

The following technologies were used:
- Programming language: Python (Pandas and NumPy);
- Relational database: MySQL;
- Versioning control: Git;
- Others: Google Sheets/Microsoft Excel.

This project:
1. Extracts data from the CSV files using Python (Pandas).
2. Transforms them for database using Python.
3. Loads them into a MySQL database using Python (NumPy).

How to run:
1. Download the following files:
    - Expenses.xlsx
	- Despesas - Contas.csv
	- Despesas - Mercado.csv
	- Despesas - Transporte.csv
2. Download the file "ETL.py" and update the MySQL connection information right at the beginning.
3. Place all files at the same directory.
4. Execute the file "ETL.py".
5. This Python script generates a database view named EXPENSES_VW, which you can query it and see the same results as can be seen in the file "Expenses.xlsx". This script also generates a file named "ExpensesQuery.sql" at the same location of the execution, this file contains the same query used in the view mentioned.
