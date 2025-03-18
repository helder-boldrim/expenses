# Title: Expenses ETL process
# Description: This script is written in Python and designed for managing expenses. It extracts data from a CSV file, transforms it for database insertion, and loads it into a MySQL database.
# Version: 1.0 - The script runs perfectly after making some manual changes to the CSV file before execution. This version aims to minimize the need for these manual adjustments.
#          1.1 - The category position in the CSV file no longer needs to be set manually. If the category is 'None,' the script will automatically assign the most recently used category.
#          1.2 - Improved category position handling when its value is null, and a query file is now generated after execution.
#          1.3 - The last row in the CSV file, which contains the totals, no longer needs to be removed manually.
#          1.4 - Cleaning script.
#          2.0 - This version aims to improve the script.
#          2.1 - Improved the error message for the column item identification.
#          2.2 - Improved the logging algorithm and messages, updated MySQL database connection credentials and cleaned the script.
#          2.3 - Added new column table for each tab on expenses spreadsheet and treated them.
#          3.0 - This version aims to translate some expense items from portuguese to english.
#          3.1 - Translated expense items from Portuguese to English.
#          3.2 - Translated remaining expense items from Portuguese to English.
try:
	import mysql.connector
	import pandas as pd
	import numpy as np 

	mydb = mysql.connector.connect(
		host="localhost",
		user="Python",
		password="pait0m",
		use_pure=True
	)

	mycursor = mydb.cursor(buffered=True)

	mycursor.execute("Use pythondb")
	sql = "Drop Table If Exists EXPENSES"
	mycursor.execute(sql)
	mycursor.execute("Create Table EXPENSES (date Varchar(7) NOT NULL, section Varchar(30) NOT NULL, category Varchar(30) NOT NULL, subcategory Varchar(30) NOT NULL, amount Double DEFAULT 0)")

	filesSet = ("Despesas - Contas.csv", "Despesas - Mercado.csv", "Despesas - Transporte.csv")

	for section in filesSet:
		df = pd.read_csv(section)
		df = df.head(-1)
		df["Categoria"].fillna(df["Categoria"], inplace = True)
		df.fillna("R$0,00", inplace = True)

		itemSection = section[:-4].replace("Despesas - ", "")
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
				else: 
					print(f"Error while trying to identify the type of the column item! Position var: {position}")
					raise StopIteration

				position += 1

			# Translation section - pt-BR to en
			match itemSection:
				case 'Contas': itemSection = 'Bills'
				case 'Mercado': itemSection = 'Grocery'
				case 'Transporte': itemSection = 'Transport'

			match itemCategory:
				case 'Moradia': itemCategory = 'Residence'
				case 'Lazer': itemCategory = 'Leisure'
				case 'Saladas': itemCategory = 'Vegetables'
				case 'Proteinas': itemCategory = 'Proteins'
				case 'Carboidratos': itemCategory = 'Carbohydrates'
				case 'Frutas': itemCategory = 'Fruits'
				case 'Laticinios': itemCategory = 'Dairy'
				case 'Higiene': itemCategory = 'Hygiene'
				case 'Itens': itemCategory = 'Items'
				case 'Carro': itemCategory = 'Car'
				case 'Moto': itemCategory = 'Motorcycle'
				case 'Outros': itemCategory = 'Others'

			match itemSubcategory:
				case 'Aluguel': itemSubcategory = 'Rent'
				case 'Condomínio': itemSubcategory = 'Service Charge'
				case 'Conta de Agua': itemSubcategory = 'Water Bill'
				case 'Conta de Luz': itemSubcategory = 'Energy Bill'
				case 'Internet': itemSubcategory = 'Internet'
				case 'Spotify': itemSubcategory = 'Music'
				case 'Netflix': itemSubcategory = 'Video'
				case 'Xbox': itemSubcategory = 'Games'
				case 'Viagens': itemSubcategory = 'Travels'
				case 'Festas': itemSubcategory = 'Parties'
				case 'Presentes': itemSubcategory = 'Gifts'
				case 'Cabelo/Barba': itemSubcategory = 'Haircut'
				case 'INSS': itemSubcategory = 'Social Security'
				case 'Internet movel': itemSubcategory = 'Mobile Internet'
				case 'Restaurantes': itemSubcategory = 'Restaurants'
				case 'Roupas': itemSubcategory = 'Clothes'
				case 'Saude/Farmacos': itemSubcategory = 'Health/Medicines'
				case 'Salada Pronta': itemSubcategory = 'Ready Salad'
				case 'Alface/Rucula': itemSubcategory = 'Lettuce/Arugula'
				case 'Brocolis/Couve flor': itemSubcategory = 'Broccoli/Cauliflower'
				case 'Cenoura/Beterraba': itemSubcategory = 'Carrot/Beetroot'
				case 'Tomate': itemSubcategory = 'Tomato'
				case 'Atum': itemSubcategory = 'Tuna'
				case 'Boi': itemSubcategory = 'Ox'
				case 'Frango': itemSubcategory = 'Chicken'
				case 'Porco': itemSubcategory = 'Pig'
				case 'Ovos': itemSubcategory = 'Eggs'
				case 'Graos': itemSubcategory = 'Grains'
				case 'Pao': itemSubcategory = 'Bread'
				case 'Banana': itemSubcategory = 'Banana'
				case 'Maca': itemSubcategory = 'Apple'
				case 'Uva': itemSubcategory = 'Grape'
				case 'Leite': itemSubcategory = 'Milk'
				case 'Manteiga': itemSubcategory = 'Butter'
				case 'Queijo': itemSubcategory = 'Cheese'
				case 'Requeijao': itemSubcategory = 'Cream Cheese'
				case 'Banho': itemSubcategory = 'Bath'
				case 'Bucal': itemSubcategory = 'Oral'
				case 'Limpeza': itemSubcategory = 'Cleaning'
				case 'Papel Higienico': itemSubcategory = 'Toilet Paper'
				case 'Pos Banho': itemSubcategory = 'Post Bath'
				case 'Banho': itemSubcategory = 'Bath'
				case 'Cama': itemSubcategory = 'Bed'
				case 'Cozinha': itemSubcategory = 'Kitchen'
				case 'Limpeza': itemSubcategory = 'Cleaning'
				case 'Agua': itemSubcategory = 'Water'
				case 'Azeite': itemSubcategory = 'Olive Oil'
				case 'Geleia/Doce de leite': itemSubcategory = 'Jelly'
				case 'Junk': itemSubcategory = 'Junk'
				case 'Refrigerante/Suco': itemSubcategory = 'Soda/Juice'
				case 'Suplementos': itemSubcategory = 'Suplements'
				case 'Temperos/Molhos': itemSubcategory = 'Spices/Sauces'
				case 'Combustivel': itemSubcategory = 'Gas'
				case 'Impostos': itemSubcategory = 'Taxes'
				case 'Lavagem': itemSubcategory = 'Washing'
				case 'Manutencao': itemSubcategory = 'Maintenance'
				case 'Pedagio': itemSubcategory = 'Toll'
				case 'Oleo': itemSubcategory = 'Oil'
				case 'Cateira Habilitacao': itemSubcategory = 'Drivers License'
				case 'Onibus': itemSubcategory = 'Bus'
				case 'Uber': itemSubcategory = 'Taxi'
				case 'Aviao': itemSubcategory = 'Plane'
				case 'Outros': itemSubcategory = 'Others'

			counter = 0
			affectedRows = 0
			for itemDate in itemDateArray:
				sql = "Insert Into EXPENSES (date, section, category, subcategory, amount) VALUES (%s, %s, %s, %s, %s)"
				val = (str(itemDate), itemSection, itemCategory, itemSubcategory, float(itemAmountArray[counter]))
				mycursor.execute(sql, val)
				counter += 1
				affectedRows += mycursor.rowcount

			print(f"{counter}/{affectedRows} rows were read/inserted for category \"{itemCategory}\" and subcategory \"{itemSubcategory}\".")

	f = open("ExpensesQuery.sql", "wt")
	queryText = "Select \n\tsection, \n\tcategory, \n\tsubcategory, "

	for itemDates in itemDateArray:
		queryText = queryText + f"\n\tSum(Case When date = '{itemDates}' Then amount End) As '{itemDates}',"

	queryText = queryText[:-1] + "\nFrom Expenses\nGroup By section, category, subcategory;"

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