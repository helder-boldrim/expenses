# Title: Expenses ETL process
# Description: This script is written in Python and designed for managing expenses. It extracts data from a CSV file, transforms it for database insertion, and loads it into a MySQL database.
# Version: 1.0 - The script runs perfectly after making some manual changes to the CSV file before execution. This version aims to minimize the need for these manual adjustments.
#          1.1 - The category position in the CSV file no longer needs to be set manually. If the category is 'None,' the script will automatically assign the most recently used category.
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

	### Table creation
	mycursor.execute("Use pythondb")
	sql = "Drop Table If Exists EXPENSES"
	mycursor.execute(sql)
	mycursor.execute("Create Table EXPENSES (date Varchar(7) NOT NULL, category Varchar(30) NOT NULL, subcategory Varchar(30) NOT NULL, amount Double DEFAULT 0)")
	
	mycursor.execute("Show Tables")
	for x in mycursor:
		print(x)

	df = pd.read_csv('Despesas - Contas.csv')
	df.fillna("R$0,00", inplace = True)  # Find a Python function to update the null amounts to R$0.00 instead of this because there are other null columns

	itemCategory = ""
	
	for i, row in df.iterrows(): # rows
		position = 0
		itemDateArray = np.array([])
		itemAmountArray = np.array([])
		for col in row: # cols
			if position >= 2: 
				itemDateArray = np.append(itemDateArray, row.index[position]) 
				itemAmountArray = np.append(itemAmountArray, [float(col.replace('R$', '').replace('.', '').replace(',', '.'))]) 
				#print(f"Value: {col.replace('R$', '').replace('.', '').replace(',', '.')}")
				#print(itemArray)
				#print(row.index[position])
			elif position == 0: itemCategory = col.replace("R$0,00", itemCategory)  #print(f"Category/{row.index[position]}: {col}")
			elif position == 1: itemSubcategory = col  #print(f"Subcategory/{row.index[position]}: {col}")
			else: print(f"Error!")

			position += 1
		
		#print("Cat: ", category)
		#print("Sub: ", subcategory)
		#print("Amt: ", itemArray)

		# Data insertion
		counter = 0
		for itemDate in itemDateArray:
			#print(itemDate, itemAmountArray[counter])
			#print(f"Insert Into EXPENSES (date, category, subcategory, amount) VALUES ({type(str(itemDate))}, {type(itemCategory)}, {type(itemSubcategory)}, {type(float(itemAmountArray[counter]))})")
			sql = "Insert Into EXPENSES (date, category, subcategory, amount) VALUES (%s, %s, %s, %s)"
			val = (str(itemDate), itemCategory, itemSubcategory, float(itemAmountArray[counter]))
			mycursor.execute(sql, val)
			counter += 1

		mydb.commit()
		print(mycursor.rowcount, "was inserted/deleted/updated. Last Row ID: ", mycursor.lastrowid)
		print()

	#df["  column  "].fillna("  replacement text  ", inplace = True)

	"""data = {
		"Categoria": ["Moradia","Moradia","Moradia"],
		"Conta": ["Aluguel","Condomínio","Internet"],
		"2024/07": ["R$1.345,89","R$150,00","R$100,00"],
		"2024/08": ["R$1.345,89","R$170,00","R$105,00"],
		"2024/09": ["R$1.431,68","R$170,00","R$105,00"],
	}

	df = pd.DataFrame(data)

	print(df.loc[2])"""

except BaseException as err:
	print('Error: ', err.args)
else:
	print("Success")
finally:
	print('Finished')