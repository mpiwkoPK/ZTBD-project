#
# import pandas as pd
# from pymongo import MongoClient
# import time
#
# #Wczytanie pliku ze zbiorem danych
# df = pd.read_csv('../dane/polaczone_dane.csv')
#
# client = MongoClient('localhost', 27017)
# db = client['ztbd_with_aggregation']
# collection = db['lego_collection']
# # Funkcja do pobierania parzystych kolorów - limit 10
# # Funkcja do pobierania parzystych kolorów - limit 10
# def get_even_colors():
#     result = db.lego_collection.find({"parts.color": {"$mod": [2, 0]}}).limit(10)
#     return result
#
# # Funkcja do pobierania wszystkich kolorów - limit 100
# def get_colors():
#     result = db.lego_collection.aggregate([
#       {"$unwind": "$parts"},
#       {"$group": {"_id": "$parts.color"}},
#       {"$limit": 100}
#     ])
#     return result
#
# # Funkcja do odczytu zestawów z poszczególnych lat + sortowanie malejąco według liczby części
# def get_sets():
#     result = db.lego_collection.aggregate([
#         {"$unwind": "$parts"},
#         {"$group": {"_id": {"name": "$set_name", "year": "$year"}, "num_parts": {"$sum": "$parts.quantity"}}},
#         {"$project": {"set_name": "$_id.name", "year": "$_id.year", "num_parts": 1, "_id": 0}},
#         {"$sort": {"num_parts": -1}}
#     ])
#     return result
#
# # Funkcja do pobierania wszystkich części w zestawie o podanym numerze
# def get_parts():
#     result = db.lego_collection.aggregate([
#         {"$match": {"set_number": "00-1"}},
#         {"$unwind": "$parts"},
#         {"$project": {"part_num": "$parts.part_num", "part_name": "$parts.name", "category_name": "$parts.category_name", "_id": 0}}
#     ])
#     return result
#
# # Funkcja do odczytu części z poszczególnych/konkretnej kategorii (setu)
# def get_part_from_set():
#     result = db.lego_collection.aggregate([
#         {"$unwind": "$parts"},
#         {"$group": {"_id": "$parts.category_name", "num_parts": {"$addToSet": "$parts.part_num"}}},
#         {"$project": {"category_name": "$_id", "num_parts": {"$size": "$num_parts"}, "_id": 0}}
#     ])
#     return result
#
# # Funkcja do odczytu najpopularniejszych motywów + ilość zestawów w tym temacie
# def get_theme():
#     result = db.lego_collection.aggregate([
#         {"$group": {"_id": "$theme_name", "num_sets": {"$sum": 1}}},
#         {"$project": {"theme_name": "$_id", "num_sets": 1, "_id": 0}},
#         {"$sort": {"num_sets": -1}}
#     ])
#     return result
#
# # Funkcja do usuwania rekordów
# # def delete_records(num_records):
# #     result = db.lego_collection.delete_many({}).limit(num_records)
# #     return result
