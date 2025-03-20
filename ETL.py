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
#          4.0 - This version aims to enhance MySQL database structure.
#          4.1 - Modified the Expenses table and created tables for Section, Category and Subcategory.
#          4.2 - Modified the database structure and data insertion algorithm.
#          4.3 - Improved the translation section algorithm.
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

	#f = open("Expenses.ddl", "rt")
	#mycursor.executescript(f.read())
	#f.close()

	mycursor.execute("Drop Table If Exists Expenses")
	mycursor.execute("Drop Table If Exists ExpensesSubcategory")
	mycursor.execute("Drop Table If Exists ExpensesCategory")
	mycursor.execute("Drop Table If Exists ExpensesSection")

	mycursor.execute("""
		Create Table ExpensesSection (
			id INT AUTO_INCREMENT, 
			name Varchar(30) NOT NULL,
			PRIMARY KEY (id),
			CONSTRAINT UC_ExpensesSection UNIQUE (name)
		)"""
	)

	mycursor.execute("""
		Create Table ExpensesCategory (
			id INT AUTO_INCREMENT, 
			name Varchar(30) NOT NULL,
			sectionId INT NOT NULL,
			PRIMARY KEY (id),
			CONSTRAINT FK_ExpensesCategorySection FOREIGN KEY (sectionId) REFERENCES ExpensesSection(id),
			CONSTRAINT UC_ExpensesCategory UNIQUE (name, sectionId)
		)"""
	)

	mycursor.execute("""
		Create Table ExpensesSubcategory (
			id INT AUTO_INCREMENT, 
			name Varchar(30) NOT NULL,
			categoryId INT NOT NULL,
			PRIMARY KEY (id),
			CONSTRAINT FK_ExpensesSubcategoryCategory FOREIGN KEY (categoryId) REFERENCES ExpensesCategory(id),
			CONSTRAINT UC_ExpensesSubcategory UNIQUE (name, categoryId)
		)"""
	)

	mycursor.execute("""
		Create Table Expenses (
			id INT AUTO_INCREMENT, 
			date Varchar(7) NOT NULL, 
			sectionId INT NOT NULL, 
			categoryId INT NOT NULL, 
			subcategoryId INT NOT NULL, 
			amount Double DEFAULT 0,
			PRIMARY KEY (id),
			CONSTRAINT FK_ExpensesSection FOREIGN KEY (sectionId) REFERENCES ExpensesSection(id),
			CONSTRAINT FK_ExpensesCategory FOREIGN KEY (categoryId) REFERENCES ExpensesCategory(id),
			CONSTRAINT FK_ExpensesSubcategory FOREIGN KEY (subcategoryId) REFERENCES ExpensesSubcategory(id),
			CONSTRAINT UC_Expenses UNIQUE (date, sectionId, categoryId, subcategoryId)
		)"""
	)

	filesSet = ("Despesas - Contas.csv", "Despesas - Mercado.csv", "Despesas - Transporte.csv")

	for section in filesSet:
		df = pd.read_csv(section)
		df = df.head(-1)
		#df["Categoria"].fillna(df["Categoria"], inplace = True)
		df.fillna("R$0,00", inplace = True)

		itemSection_ptBR = section[:-4].replace("Despesas - ", "")
		itemCategory_ptBR = ""
		itemDateArray = np.array([])
		
		for i, row in df.iterrows(): 
			position = 0
			itemDateArray = np.array([])
			itemAmountArray = np.array([])
			for col in row: 
				if position >= 2: 
					itemDateArray = np.append(itemDateArray, row.index[position])
					itemAmountArray = np.append(itemAmountArray, float(col.replace('R$', '').replace('.', '').replace(',', '.')))
				elif position == 0: itemCategory_ptBR = col.replace("R$0,00", itemCategory_ptBR)
				elif position == 1: itemSubcategory = col
				else: 
					print(f"Error while trying to identify the type of the column item! Position var: {position}")
					raise StopIteration

				position += 1

			# Translation section | pt-BR to en | Once the performance gets descreased, the algorithm for this will be changed
			match itemSection_ptBR:
				case 'Contas': 
					itemSection = 'Bills'
					match itemCategory_ptBR:
						case 'Moradia': 
							itemCategory = 'Residence'
							match itemSubcategory:
								case 'Aluguel': itemSubcategory = 'Rent'
								case 'Condomínio': itemSubcategory = 'Service Charge'
								case 'Conta de Agua': itemSubcategory = 'Water Bill'
								case 'Conta de Luz': itemSubcategory = 'Energy Bill'
								case 'Internet': itemSubcategory = 'Internet'

						case 'Lazer': 
							itemCategory = 'Leisure'
							match itemSubcategory:
								case 'Spotify': itemSubcategory = 'Music'
								case 'Netflix': itemSubcategory = 'Video'
								case 'Xbox': itemSubcategory = 'Games'
								case 'Viagens': itemSubcategory = 'Travels'
								case 'Festas': itemSubcategory = 'Parties'
								case 'Presentes': itemSubcategory = 'Gifts'

						case 'Outros': 
							itemCategory = 'Others'
							match itemSubcategory:
								case 'Cabelo/Barba': itemSubcategory = 'Haircut'
								case 'INSS': itemSubcategory = 'Social Security'
								case 'Internet movel': itemSubcategory = 'Mobile Internet'
								case 'Restaurantes': itemSubcategory = 'Restaurants'
								case 'Roupas': itemSubcategory = 'Clothes'
								case 'Saude/Farmacos': itemSubcategory = 'Health/Medicines'
			
				case 'Mercado': 
					itemSection = 'Grocery'
					match itemCategory_ptBR:	
						case 'Saladas': 
							itemCategory = 'Vegetables'
							match itemSubcategory:
								case 'Salada Pronta': itemSubcategory = 'Ready Salad'
								case 'Alface/Rucula': itemSubcategory = 'Lettuce/Arugula'
								case 'Brocolis/Couve flor': itemSubcategory = 'Broccoli/Cauliflower'
								case 'Cenoura/Beterraba': itemSubcategory = 'Carrot/Beetroot'
								case 'Tomate': itemSubcategory = 'Tomato'

						case 'Proteinas': 
							itemCategory = 'Proteins'
							match itemSubcategory:
								case 'Atum': itemSubcategory = 'Tuna'
								case 'Boi': itemSubcategory = 'Ox'
								case 'Frango': itemSubcategory = 'Chicken'
								case 'Porco': itemSubcategory = 'Pig'
								case 'Ovos': itemSubcategory = 'Eggs'

						case 'Carboidratos': 
							itemCategory = 'Carbohydrates'
							match itemSubcategory:
								case 'Graos': itemSubcategory = 'Grains'
								case 'Pao': itemSubcategory = 'Bread'
								case 'Outros': itemSubcategory = 'Others'

						case 'Frutas': 
							itemCategory = 'Fruits'
							match itemSubcategory:
								case 'Banana': itemSubcategory = 'Banana'
								case 'Maca': itemSubcategory = 'Apple'
								case 'Uva': itemSubcategory = 'Grape'

						case 'Laticinios': 
							itemCategory = 'Dairy'
							match itemSubcategory:
								case 'Leite': itemSubcategory = 'Milk'
								case 'Manteiga': itemSubcategory = 'Butter'
								case 'Queijo': itemSubcategory = 'Cheese'
								case 'Requeijao': itemSubcategory = 'Cream Cheese'
								case 'Outros': itemSubcategory = 'Others'

						case 'Higiene': 
							itemCategory = 'Hygiene'
							match itemSubcategory:
								case 'Banho': itemSubcategory = 'Bath'
								case 'Bucal': itemSubcategory = 'Oral'
								case 'Limpeza': itemSubcategory = 'Cleaning'
								case 'Papel': itemSubcategory = 'Paper'
								case 'Pos Banho': itemSubcategory = 'Post Bath'
								case 'Outros': itemSubcategory = 'Others'

						case 'Itens': 
							itemCategory = 'Items'
							match itemSubcategory:
								case 'Banho': itemSubcategory = 'Bath'
								case 'Cama': itemSubcategory = 'Bed'
								case 'Cozinha': itemSubcategory = 'Kitchen'
								case 'Limpeza': itemSubcategory = 'Cleaning'

						case 'Outros': 
							itemCategory = 'Others'
							match itemSubcategory:
								case 'Agua': itemSubcategory = 'Water'
								case 'Azeite': itemSubcategory = 'Olive Oil'
								case 'Geleia/Doce de leite': itemSubcategory = 'Jelly'
								case 'Junk': itemSubcategory = 'Junk'
								case 'Refrigerante/Suco': itemSubcategory = 'Soda/Juice'
								case 'Suplementos': itemSubcategory = 'Suplements'
								case 'Temperos/Molhos': itemSubcategory = 'Spices/Sauces'

				case 'Transporte': 
					itemSection = 'Transport'
					match itemCategory_ptBR:	
						case 'Carro': 
							itemCategory = 'Car'
							match itemSubcategory:
								case 'Combustivel': itemSubcategory = 'Gas'
								case 'Impostos': itemSubcategory = 'Taxes'
								case 'Lavagem': itemSubcategory = 'Washing'
								case 'Manutencao': itemSubcategory = 'Maintenance'
								case 'Pedagio': itemSubcategory = 'Toll'
								case 'Oleo': itemSubcategory = 'Oil'

						case 'Moto': 
							itemCategory = 'Motorcycle'
							match itemSubcategory:
								case 'Combustivel': itemSubcategory = 'Gas'
								case 'Impostos': itemSubcategory = 'Taxes'
								case 'Lavagem': itemSubcategory = 'Washing'
								case 'Manutencao': itemSubcategory = 'Maintenance'
								case 'Pedagio': itemSubcategory = 'Toll'
								case 'Oleo': itemSubcategory = 'Oil'

						case 'Outros': 
							itemCategory = 'Others'
							match itemSubcategory:
								case 'Cateira Habilitacao': itemSubcategory = 'Drivers License'
								case 'Onibus': itemSubcategory = 'Bus'
								case 'Uber': itemSubcategory = 'Taxi'
								case 'Aviao': itemSubcategory = 'Plane'

			# Database data insertion - Section
			mycursor.execute(("Select id From ExpensesSection Where name = '%s'")%(itemSection))
			itemSectionId = mycursor.fetchone()
			if itemSectionId is None: 
				mycursor.execute(("Insert Into ExpensesSection (name) Values ('%s')")%(itemSection))
				mycursor.execute(("Select id From ExpensesSection Where name = '%s'")%(itemSection))
				itemSectionId = mycursor.fetchone()

			# Database data insertion - Category
			mycursor.execute(("Select id From ExpensesCategory Where name = '%s' And sectionId = %s")%(itemCategory, itemSectionId[0]))
			itemCategoryId = mycursor.fetchone()
			if itemCategoryId is None: 
				mycursor.execute(("Insert Into ExpensesCategory (name, sectionId) Values ('%s', %s)")%(itemCategory, itemSectionId[0]))
				mycursor.execute(("Select id From ExpensesCategory Where name = '%s' And sectionId = %s")%(itemCategory, itemSectionId[0]))
				itemCategoryId = mycursor.fetchone()

			# Database data insertion - Subcategory
			mycursor.execute(("Select id From ExpensesSubcategory Where name = '%s' And categoryId = %s")%(itemSubcategory, itemCategoryId[0]))
			itemSubcategoryId = mycursor.fetchone()
			if itemSubcategoryId is None: 
				mycursor.execute(("Insert Into ExpensesSubcategory (name, categoryId) Values ('%s', %s)")%(itemSubcategory, itemCategoryId[0]))
				mycursor.execute(("Select id From ExpensesSubcategory Where name = '%s' And categoryId = %s")%(itemSubcategory, itemCategoryId[0]))
				itemSubcategoryId = mycursor.fetchone()

			# Database data insertion - Expenses
			affectedRows = 0
			counter = 0
			for itemDate in itemDateArray:
				sql = "Insert Into Expenses (date, sectionid, categoryid, subcategoryid, amount) VALUES (%s, %s, %s, %s, %s)"
				val = (str(itemDate), itemSectionId[0], itemCategoryId[0], itemSubcategoryId[0], float(itemAmountArray[counter]))
				mycursor.execute(sql, val)
				counter += 1
				affectedRows += mycursor.rowcount

			print(f"{counter}/{affectedRows} rows were read/inserted for section \"{itemSection}\" / category \"{itemCategory}\" / subcategory \"{itemSubcategory}\".")

	mycursor.execute("Delete From Expenses Where amount = 0")

	# Query file creation
	f = open("ExpensesQuery.sql", "wt")
	queryText = "Select \n\tsec.name As 'Section', \n\tcat.name As 'Category', \n\tsub.name As 'Subcategory', "

	for itemDates in itemDateArray:
		queryText = queryText + f"\n\tIfNull(Sum(Case When exp.date = '{itemDates}' Then exp.amount End), 0) As '{itemDates}',"

	queryText = queryText[:-1] + "\nFrom Expenses exp\n\tRight Join ExpensesSubcategory sub On exp.subcategoryId = sub.id\n\tInner Join ExpensesCategory cat On sub.categoryId = cat.id\n\tInner Join ExpensesSection sec On cat.sectionId = sec.id\nGroup By sec.name, cat.name, sub.name\nOrder By sec.id Asc, cat.id Asc, sub.id Asc;"

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