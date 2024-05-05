import pandas as pd
from pymongo import MongoClient
import time

#Wczytanie pliku ze zbiorem danych
df = pd.read_csv('../dane/polaczone_dane.csv')

client = MongoClient('localhost', 27017)
db = client['ztbd_with_aggregation']
collection = db['lego_collection']

#Tworzenie dokumentu na podstawie zbioru danych
def create_part_document(row):
    part_document = {
        "part_num": row['part_num'],
        "name": row['part_name'],
        "category_name": row['part_category_name'],
        "color": row['colors_name'],
        "rgb": row['rgb'],
        "is_trans": row['is_trans'] == 't',
        "quantity": int(row['quantity']),
        "is_spare": row['is_spare'] == 't'
    }
    return part_document

start_time = time.time()

#Grupowanie części według numeru zestawu (set_num)
grouped = df.groupby('set_num')

#Iteracja po kazdym zestawie
for set_num, group in grouped:

    #Tworzenie dokumentu dla zestawu
    set_document = {
        "set_number": set_num,
        "version": int(group['version'].iloc[0]),
        "year": int(group['year'].iloc[0]),
        "theme_name": group['themes_name'].iloc[0],
        "num_parts": int(group['num_parts'].iloc[0]),
        "parts": []
    }
    #Dodanie części do dokumentu zestawu
    for _, row in group.iterrows():
        part_document = create_part_document(row)
        set_document['parts'].append(part_document)

    #Dodanie dokumentu zestawu do bazy danych
    collection.insert_one(set_document)

print("Proces zakończony. Zestawy i ich części zostały dodane do bazy danych MongoDB.")

end_time = time.time()
execution_time = end_time - start_time

print("Czas wykonania funkcji:", execution_time, "sekund")