# Title: Expenses ETL process
# Description: This script is written in Python and designed for managing expenses. It extracts data from a CSV file, transforms it for database insertion, and loads it into a MySQL database.
# Version: 1.0 - The script runs perfectly after making some manual changes to the CSV file before execution. This version aims to minimize the need for these manual adjustments.
#          1.1 - The category position in the CSV file no longer needs to be set manually. If the category is 'None,' the script will automatically assign the most recently used category.
#          1.2 - Improved category position handling when its value is null, and a query file is now generated after execution.
#          1.3 - The last row in the CSV file, which contains the totals, no longer needs to be removed manually.
#          1.4 - Cleaning script.
try:
	import mysql.connector
	import pandas as pd
	import numpy as np 

	mydb = mysql.connector.connect(
		host="localhost",
		user="Helder",
		password="H85qual#",
		use_pure=True
	)

	mycursor = mydb.cursor(buffered=True)

	mycursor.execute("Use pythondb")
	sql = "Drop Table If Exists EXPENSES"
	mycursor.execute(sql)
	mycursor.execute("Create Table EXPENSES (date Varchar(7) NOT NULL, category Varchar(30) NOT NULL, subcategory Varchar(30) NOT NULL, amount Double DEFAULT 0)")

	df = pd.read_csv('Despesas - Contas.csv')
	df = df.head(-1)
	df["Categoria"].fillna(df["Categoria"], inplace = True)
	df.fillna("R$0,00", inplace = True)

	itemCategory = ""
	itemDateArray = np.array([])
	
	for i, row in df.iterrows(): 
		position = 0
		itemDateArray = np.array([])
		itemAmountArray = np.array([])
		for col in row: 
			if position >= 2: 
				itemDateArray = np.append(itemDateArray, row.index[position])
				itemAmountArray = np.append(itemAmountArray, [float(col.replace('R$', '').replace('.', '').replace(',', '.'))])
			elif position == 0: itemCategory = col.replace("R$0,00", itemCategory)
			elif position == 1: itemSubcategory = col
			else: print(f"Error!")

			position += 1

		counter = 0
		for itemDate in itemDateArray:
			sql = "Insert Into EXPENSES (date, category, subcategory, amount) VALUES (%s, %s, %s, %s)"
			val = (str(itemDate), itemCategory, itemSubcategory, float(itemAmountArray[counter]))
			mycursor.execute(sql, val)
			counter += 1

		#print(mycursor.rowcount, "was inserted/deleted/updated. Last Row ID: ", mycursor.lastrowid)
		print(counter, "was inserted/deleted/updated. Last Row ID: ", mycursor.lastrowid)
		print()

	f = open("ExpensesQuery.sql", "wt")
	queryText = "Select \n\tcategory, \n\tsubcategory, "

	for itemDates in itemDateArray:
		queryText = queryText + f"\n\tSum(Case When date = '{itemDates}' Then amount End) As '{itemDates}',"

	queryText = queryText[:-1] + "\nFrom Expenses\nGroup By category, subcategory;"

	f.write(queryText)
	f.close()
except BaseException as err:
	print('Error: ', err.args)
	mydb.rollback()
else:
	print("Success")
	mydb.commit()
finally:
	print('Finished')