import psycopg2
import csv
import time

# Połączenie z bazą danych PostgreSQL
connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="example",
    database="lego_"
)
cursor = connection.cursor()

# Funkcja do tworzenia tabeli w bazie danych
def create_table(cursor, table_name, columns):
    column_str = ', '.join(columns)
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_str})"
    cursor.execute(query)

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
            if 'parent_id' in columns and row[columns.index('parent_id')] == '':
                row[columns.index('parent_id')] = None
            if not check_func or check_func(cursor, row[columns.index('part_num')]):
                placeholders = ', '.join(['%s'] * len(row))
                query = f"INSERT INTO {tablename} VALUES ({placeholders})"
                cursor.execute(query, row)
        print(f"Dane z pliku {filename} zostały dodane do tabeli {tablename}.")
    
    connection.commit()

def load_sets_part_data(cursor, filename):
    load_data_from_csv(cursor, filename, 'Sets_part', ['inventory_id', 'part_num', 'color_id', 'quantity', 'is_spare'], part_num_exists)

# Definicje tabel i ich kolumn
tables = {
    'Themes': ['ID INT PRIMARY KEY', 'name VARCHAR(100)', 'parent_id INT'],
    'Part_categories': ['ID VARCHAR(20) PRIMARY KEY', 'name VARCHAR(100)'],
    'Colors': ['ID INT PRIMARY KEY', 'name VARCHAR(50)', 'rgb VARCHAR(10)', 'is_trans BOOLEAN'],
    'Sets': ['set_num VARCHAR(20) PRIMARY KEY', 'name VARCHAR(100)', 'year INT', 'theme_id INT', 'num_parts INT', 'FOREIGN KEY (theme_id) REFERENCES Themes(ID)'],
    'Parts': ['part_num VARCHAR(20) PRIMARY KEY', 'name VARCHAR(255)', 'part_cat_id VARCHAR(20)', 'FOREIGN KEY (part_cat_id) REFERENCES Part_categories(ID)'],
    'inventories': ['ID INT PRIMARY KEY', 'version INT', 'set_num VARCHAR(20)', 'FOREIGN KEY (set_num) REFERENCES Sets(set_num)'],
    'Sets_part': ['inventory_id INT', 'part_num VARCHAR(20)', 'color_id INT', 'quantity INT', 'FOREIGN KEY (inventory_id) REFERENCES inventories(ID)', 'FOREIGN KEY (part_num) REFERENCES Parts(part_num)', 'FOREIGN KEY (color_id) REFERENCES Colors(ID)', 'is_spare BOOLEAN']
}

start_time = time.time()
# Tworzenie tabel w bazie
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

end_time = time.time()
execution_time = end_time - start_time

print("Czas wykonania funkcji:", execution_time, "sekund")

# Zamykanie połączenia z bazą danych
cursor.close()
connection.close()