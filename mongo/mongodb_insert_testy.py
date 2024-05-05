import pandas as pd
from pymongo import MongoClient
import time

#Wczytanie pliku ze zbiorem danych
df = pd.read_csv('../dane/polaczone_dane.csv')

# Połącz się z bazą danych MongoDB
client = MongoClient('localhost', 27017)
db = client['ztbd_without_aggregation']
collection = db['lego_collection']

#Tworzenie dokumentu na podstawie zbioru danych bez zagniezdzen
def row_to_mongodb_document(row):
    document = {
        "set_number": row['set_num'],
        "version": int(row['version']),
        "year": int(row['year']),
        "theme_name": row['themes_name'],
        "num_parts": int(row['num_parts']),
        "parts": [
            {
                "part_num": row['part_num'],
                "name": row['part_name'],
                "category_name": row['part_category_name'],
                "color": row['colors_name'],
                "rgb": row['rgb'],
                "is_trans": True if row['is_trans'] == 't' else False,
                "quantity": int(row['quantity']),
                "is_spare": True if row['is_spare'] == 't' else False
            }
        ]
    }
    return document

rows = [10, 100, 1000, 5000]

for k in range(0, 15):
    for i in rows:
        j = 0
        start_time = time.time()

        #Dodanie kazdego wiersza ze zbioru danych do mongodb, rzutujac go na strukture dokumentu MongoDB
        for _, row in df.iterrows():
            if j == i:
                break
            document = row_to_mongodb_document(row)
            collection.insert_one(document)
            j += 1
        print("Dane zostały dodane do kolekcji MongoDB.")

        end_time = time.time()
        execution_time = end_time - start_time

        print("Czas wykonania funkcji:", execution_time, "sekund")
