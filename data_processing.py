import pandas as pd
import numpy as np
import pymongo

#Wczytanie plikow csv zbioru danych
colors = pd.read_csv('dane/colors.csv')
inventories = pd.read_csv('dane/inventories.csv')
inventory_parts = pd.read_csv('dane/inventory_parts.csv')
inventory_sets = pd.read_csv('dane/inventory_sets.csv')
part_categories = pd.read_csv('dane/part_categories.csv')
parts = pd.read_csv('dane/parts.csv')
sets = pd.read_csv('dane/sets.csv')
themes = pd.read_csv('dane/themes.csv')

#Zmiana powtarzajacych sie nazw kolumn
themes.rename(columns={'id': 'themes_id', 'name': 'themes_name'}, inplace = True)
sets.rename(columns={'name': 'sets_name'}, inplace = True)
inventories.rename(columns={'id': 'inventories_id'}, inplace = True)
colors.rename(columns={'name': 'colors_name'}, inplace = True)
parts.rename(columns={'name': 'part_name'}, inplace= True)
part_categories.rename(columns={'name': 'part_category_name'}, inplace = True)

#Laczenie plikow csv na podstawie wspolnych kolumn
merged_data = pd.merge(inventory_parts, parts, left_on='part_num', right_on='part_num', how='inner')
merged_data = pd.merge(merged_data, part_categories, left_on='part_cat_id', right_on='id', how='inner')
merged_data = pd.merge(merged_data, colors, left_on='color_id', right_on='id', how='inner')
merged_data = pd.merge(inventories, merged_data, left_on='inventories_id', right_on='inventory_id', how='inner')
sets.rename(columns={'set_num': 'id', 'name': 'set_name'}, inplace = True)
merged_data = pd.merge(merged_data, sets, left_on='set_num', right_on='id', how='inner')
merged_data = pd.merge(merged_data, themes, left_on='theme_id', right_on='themes_id', how='inner')


#Usuwanie nadmiarowych id, niepotrzebnych dla mongodb
merged_data.drop(columns=['inventories_id', 'inventory_id', 'color_id', 'part_cat_id', 'id_x', 'id_y', 'id', 'theme_id', 'themes_id', 'parent_id'], inplace=True)

merged_data.to_csv('dane/polaczone_dane.csv', index=False)

#print(inventories.head())

#dane = pd.merge(parts, part_categories, on='')
#print(colors.describe(include = "all"))
#print(inventories[inventories['id'] == 18438])
#print(sets[sets['set_num'] == "40179-1"])
#print(inventory_parts[inventory_parts['quantity'] == 900])



# print(sets[sets['set_num'] == "40179-1"])
# print(inventories[inventories['set_num'] == "40179-1"])
# print(inventory_parts[inventory_parts['inventory_id'] == 18438])
#
# print(inventories[inventories['id'] == 18438])
#
# print(inventories[inventories['version'] == 2])
# print(inventories[inventories['set_num'] == "6515-1"])
#
# print(inventory_parts[inventory_parts['inventory_id'] == 2523])
# print(inventory_parts[inventory_parts['inventory_id'] == 16490])


# print(part_categories['name'].value_counts())
# print(inventory_sets['quantity'].value_counts())




# client = pymongo.MongoClient('localhost', 27017)
# db = client['moja_baza_danych']
# collection = db['moja_kolekcja']
#
# doc = {
#   "set_number": "3010-1",
#   "version": 1,
#   "year": 2010,
#   "theme_name": "Superman",
#   "num_parts": 4051,
#   "parts": [
#     {
#       "part_num": "399292",
#       "name": "Triblock",
#       "category_name": "Lego",
#       "color": "green",
#       "rgb": "#040292",
#       "is_trans": True,
#       "quantity": 5,
#       "is_spare": False
#     },
#     {
#       "part_num": "5678",
#       "name": "Duoblock",
#       "category_name": "Tools",
#       "color": "blue",
#       "rgb": "#522982",
#       "is_trans": True,
#       "quantity": 10,
#       "is_spare": False
#     }
#   ]
# }
# collection.insert_one(doc)
# print("Dokument został dodany do kolekcji.")






