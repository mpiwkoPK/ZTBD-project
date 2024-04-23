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
create_database(cursor, 'lego_part_relations')

# Zmiana na korzystanie z bazy danych 'lego'
connection.database = 'lego_part_relations'

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


# Funkcja do wczytywania danych dla każdej tabeli z odpowiednich plików CSV
def load_data_for_tables(cursor, table_files):
    for table_name, filename in table_files.items():
        print(f"\nWczytywanie danych do tabeli {table_name}:")
        start_time = time.time()
        load_data_from_csv(cursor, filename, table_name, None, part_num_exists)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Czas wykonania dla tabeli {table_name}: {execution_time} sekund")

# Definicje plików CSV dla każdej tabeli
table_files = {
    'Themes': 'dane/Themes.csv',
    'Part_categories': 'dane/part_categories.csv',
    'Colors': 'dane/colors.csv',
    'Sets': 'dane/sets.csv',
    'Parts': 'dane/parts.csv',
    'inventories': 'dane/inventories.csv',
    'Sets_part': 'dane/inventory_parts.csv'
}

start_time = time.time()
# Tworzenie tabel w bazie danych
for table_name in table_files.keys():
    create_table(cursor, table_name, None)

# Wczytywanie danych z plików CSV do odpowiednich tabel
load_data_for_tables(cursor, table_files)

end_time = time.time()
execution_time = end_time - start_time

print("Czas wykonania funkcji:", execution_time, "sekund")

# Zamykanie połączenia z bazą danych
cursor.close()
connection.close()