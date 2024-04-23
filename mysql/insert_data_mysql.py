import mysql.connector
import csv
import time
from mysql.connector import IntegrityError

def create_database(cursor, database_name):
    query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    cursor.execute(query)
    print(f"Baza danych {database_name} została utworzona lub już istnieje.")

# Połączenie z bazą danych MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="example"
)
cursor = connection.cursor()

# Tworzenie bazy danych, jeśli nie istnieje
create_database(cursor, 'lego')

# Zmiana na korzystanie z bazy danych 'lego'
connection.database = 'lego'

# Funkcja do tworzenia tabeli w bazie danych
def create_table(cursor, table_name, columns):
    column_str = ', '.join(columns)
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_str})"
    cursor.execute(query)

# Funkcja sprawdzająca, czy dany numer części już istnieje w tabeli Parts
def part_num_exists(cursor, part_num):
    query = f"SELECT EXISTS(SELECT 1 FROM Parts WHERE Parts.part_num = %s)"
    cursor.execute(query, (part_num,))
    return cursor.fetchone()[0]

# Funkcja do wczytywania danych z pliku CSV do bazy danych
def load_data_from_csv(cursor, filename, tablename, columns, check_func=None):
    with open(filename, 'r', encoding='utf-8') as csvfile:
        csvreader = csv.reader(csvfile)
        next(csvreader)  # Pominięcie nagłówka w pliku CSV
        for row in csvreader:
            try:
                for i, val in enumerate(row):
                    if val == '':  # Jeśli wartość jest pusta, zastąp ją wartością None
                        row[i] = None
                    elif val == 'f':
                        row[i] = 0
                    elif val == 't':
                        row[i] = 1
                        
                if not check_func or check_func(cursor, row[columns.index('part_num')]):
                    if tablename == 'Parts':
                        if len(row[columns.index('name')]) > 200:
                            print(f"Dane w kolumnie 'name' są zbyt długie. Pomijanie...")
                            continue
                    
                    placeholders = ', '.join(['%s'] * len(row))
                
                    query = f"INSERT INTO {tablename} VALUES ({placeholders})"
                    cursor.execute(query, row)
                    

                
            except IntegrityError as e:
                print(f"Próba wstawienia duplikatu do tabeli {tablename}. Pomijanie...")

    connection.commit()
    print(f"Dane z pliku {filename} zostały dodane do tabeli {tablename}.")


def load_sets_part_data(cursor, filename):
    load_data_from_csv(cursor, filename, 'Sets_part', ['inventory_id', 'part_num', 'color_id', 'quantity', 'is_spare'], part_num_exists)


tables = {
    'Themes': ['ID INT PRIMARY KEY', 'name VARCHAR(100)', 'parent_id INT'],
    'Part_categories': ['ID VARCHAR(20) PRIMARY KEY', 'name VARCHAR(100)'],
    'Colors': ['ID INT PRIMARY KEY', 'name VARCHAR(50)', 'rgb VARCHAR(10)', 'is_trans BOOLEAN'],
    'Sets': ['set_num VARCHAR(20) PRIMARY KEY', 'name VARCHAR(200)', 'year INT', 'theme_id INT', 'num_parts INT', 'FOREIGN KEY (theme_id) REFERENCES Themes(ID)'],
    'Parts': ['part_num VARCHAR(20) PRIMARY KEY', 'name VARCHAR(200)', 'part_cat_id VARCHAR(20)', 'FOREIGN KEY (part_cat_id) REFERENCES Part_categories(ID)'],
    'inventories': ['ID INT PRIMARY KEY', 'version INT', 'set_num VARCHAR(20)', 'FOREIGN KEY (set_num) REFERENCES Sets(set_num)'],
    'Sets_part': ['inventory_id INT', 'part_num VARCHAR(20)', 'color_id INT', 'quantity INT', 'FOREIGN KEY (inventory_id) REFERENCES inventories(ID)', 'FOREIGN KEY (part_num) REFERENCES Parts(part_num)', 'FOREIGN KEY (color_id) REFERENCES Colors(ID)', 'is_spare BOOLEAN']
}
# Definicje plików CSV dla każdej tabeli
start_time = time.time()

#tworzenie tabel w bazie
for table_name, columns in tables.items():
    create_table(cursor, table_name, columns)

# Wczytywanie danych z plików CSV do odpowiednich tabel
load_data_from_csv(cursor, 'dane/Themes.csv', 'Themes', ['ID', 'name', 'parent_id'])
load_data_from_csv(cursor, 'dane/part_categories.csv', 'Part_categories', ['ID', 'name'])
load_data_from_csv(cursor, 'dane/colors.csv', 'Colors', ['ID', 'name', 'rgb', 'is_trans'])
load_data_from_csv(cursor, 'dane/sets.csv', 'Sets', ['set_num', 'name', 'year', 'theme_id', 'num_parts'])
load_data_from_csv(cursor, 'dane/parts.csv', 'Parts', ['part_num', 'name', 'part_cat_id'])
load_data_from_csv(cursor, 'dane/inventories.csv', 'inventories', ['ID', 'version', 'set_num'])
load_sets_part_data(cursor, 'dane/inventory_parts.csv')
#load_data_from_csv(cursor, 'dane/inventory_parts.csv', 'Sets_part', ['inventory_id', 'part_num', 'color_id', 'quantity', 'is_spare'])

end_time = time.time()
execution_time = end_time - start_time
print("Czas wykonania funkcji:", execution_time, "sekund")
