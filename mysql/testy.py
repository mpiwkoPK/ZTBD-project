import mysql.connector
import time
from mysql.connector import IntegrityError

#podłączenie do bazy ze znormalizowanymi danymi
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="example",
    database="lego"
)

#ogolna funkcja do mierzenia czasu
def count_time(text, function, *args):
    start_time = time.time()
    function(*args)
    end_time = time.time()
    execution_time = end_time - start_time
    print("Czas wykonania funkcji dla", text, ":", execution_time, "sekund")

#funkcja ktora zawiera w sobie wszystko do wysłania zapytania
def schema(query):
    cursor = connection.cursor()
    query_string = f"{query}"
    cursor.execute(query_string)
    items = cursor.fetchall()
    cursor.close()
    return items

#funkcja do pobierania kolorów o parzystych ID - 10 rekordów
def get_even_colors():
    query = "SELECT * FROM Colors WHERE ID % 2 = 0 LIMIT 10"
    schema(query)
count_time("parzystych ID kolorów limit 10", get_even_colors)

#funkcja do pobierania wszystkich kolorów - 100 rekordów
def get_colors():
    query = "SELECT * FROM Colors LIMIT 100"
    schema(query)
count_time("wszystkich kolorów limit 100", get_colors)

## skomplikowane selecty
# odczytanie zestawów z poszczególnych lat + sortowanie malejąco według liczby części 
def get_sets():
    query = "SELECT s.name AS set_name, s.year, COUNT(sp.part_num) AS num_parts FROM Sets s JOIN inventories i ON s.set_num = i.set_num JOIN Sets_part sp ON i.id = sp.inventory_id GROUP BY s.name, s.year ORDER BY num_parts DESC;"
    schema(query)
count_time("odczytania zestawów z poszczególnych lat", get_sets)

#wszystkie części w zestawie o podanym numerze
def get_parts():
    query = "SELECT p.part_num, p.name AS part_name, c.name AS category_name FROM Sets_part sp JOIN Parts p ON sp.part_num = p.part_num JOIN Part_categories c ON p.part_cat_id = c.id WHERE sp.inventory_id = 26;"
    schema(query)
count_time("wszystkich części w zestawie o podanym nr", get_parts)

# Odczytanie części z poszczególnych/konkretnej kategorii (setu)
def get_part_from_set():
    query = "SELECT pc.name AS category_name, COUNT(DISTINCT p.part_num) AS num_parts FROM Part_categories pc JOIN Parts p ON pc.id = p.part_cat_id GROUP BY pc.name;"
    schema(query)
count_time("odczytania części z konkretnego zestawu", get_part_from_set)

# najpopularniejsze motywy + ilosc zestawow w tym temacie 
def get_theme():
    query = "SELECT t.name AS theme_name, COUNT(s.set_num) AS num_sets FROM Themes t JOIN Sets s ON t.id = s.theme_id GROUP BY t.name ORDER BY num_sets DESC;"
    schema(query)
count_time("odczytania najpopularniejszych motywów", get_part_from_set)

connection.autocommit = False

#delete
def delete_records(num_records):
    query = f"DELETE FROM Sets_part LIMIT {num_records}"
    schema(query)
count_time("usunięcia 10 rekordów: ", delete_records, 10)
count_time("usunięcia 100 rekordów: ", delete_records, 100)
count_time("usunięcia 1000 rekordów: ", delete_records, 1000)


# Zamykanie połączenia
connection.close()