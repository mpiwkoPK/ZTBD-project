import psycopg2
import random
import string
import time

def create_database(database_name):
    connection = None
    try:
        connection = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="example"
        )
        connection.autocommit = True  # Wyłączamy tryb transakcyjny

        cursor = connection.cursor()

        # Sprawdź, czy baza danych już istnieje
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{database_name}'")
        exists = cursor.fetchone()

        if not exists:
            cursor.execute(f"CREATE DATABASE {database_name}")
            print(f"Baza danych {database_name} została utworzona.")
        else:
            print(f"Baza danych {database_name} już istnieje.")
    except psycopg2.Error as e:
        print("Błąd podczas tworzenia bazy danych:", e)
    finally:
        if connection:
            connection.close()

create_database('lego')
# Połączenie z bazą danych PostgreSQL
connection = psycopg2.connect(
    host="localhost",
    user="postgres",
    password="example",
    dbname="lego"
)

connection.autocommit = True
cursor = connection.cursor()

# # Zamknięcie połączenia i ponowne otwarcie nowej bazy danych
# cursor.close()
# connection.close()

# connection = psycopg2.connect(
#     host="localhost",
#     user="your_username",
#     password="your_password",
#     dbname="lego"  # Nazwa nowo utworzonej bazy danych
# )
# cursor = connection.cursor()

# Funkcja do tworzenia tabeli w bazie danych
def create_table(cursor, table_name, columns):
    column_str = ', '.join(columns)
    query = f"CREATE TABLE IF NOT EXISTS {table_name} ({column_str})"
    cursor.execute(query)

# Funkcja do wstawiania danych do tabeli
def insert_data(cursor, table_name, columns, values):
    placeholders = ', '.join(['%s'] * len(values))
    columns_str = ', '.join(columns)
    query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    cursor.execute(query, values)

tables = {
    'Themes': ['ID SERIAL PRIMARY KEY', 'name VARCHAR(100)', 'parent_id INT'],
    'Part_categories': ['ID SERIAL PRIMARY KEY', 'name VARCHAR(100)'],
    'Colors': ['ID SERIAL PRIMARY KEY', 'name VARCHAR(50)', 'rgb VARCHAR(10)', 'is_trans BOOLEAN'],
    'Sets': ['set_num VARCHAR(20) PRIMARY KEY', 'name VARCHAR(200)', 'year INT', 'theme_id INT REFERENCES Themes(ID)', 'num_parts INT'],
    'Parts': ['part_num VARCHAR(20) PRIMARY KEY', 'name VARCHAR(200)', 'part_cat_id INT REFERENCES Part_categories(ID)'],
    'inventories': ['ID SERIAL PRIMARY KEY', 'version INT', 'set_num VARCHAR(20) REFERENCES Sets(set_num)'],
    'Sets_part': ['inventory_id INT REFERENCES inventories(ID)', 'part_num VARCHAR(20) REFERENCES Parts(part_num)', 'color_id INT REFERENCES Colors(ID)', 'quantity INT', 'is_spare BOOLEAN']
}

# Tworzenie tabel w bazie danych
for table_name, columns in tables.items():
    create_table(cursor, table_name, columns)

# Funkcja do generowania losowych danych
def generate_random_string(length=10):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for i in range(length))

def generate_random_theme(cursor, theme_id):
    name = generate_random_string(10)
    parent_id = random.randint(1, theme_id) if theme_id > 1 else None
    insert_data(cursor, 'Themes', ['name', 'parent_id'], [name, parent_id])

def generate_random_part_category(cursor, part_cat_id):
    name = generate_random_string(10)
    insert_data(cursor, 'Part_categories', ['name'], [name])

def generate_random_color(cursor, color_id):
    name = generate_random_string(10)
    rgb = ''.join(random.choice('0123456789ABCDEF') for i in range(6))
    is_trans = random.choice([True, False])
    insert_data(cursor, 'Colors', ['name', 'rgb', 'is_trans'], [name, rgb, is_trans])

def generate_random_set(cursor, set_num, theme_id):
    name = generate_random_string(20)
    year = random.randint(2000, 2022)
    num_parts = random.randint(50, 500)
    insert_data(cursor, 'Sets', ['set_num', 'name', 'year', 'theme_id', 'num_parts'],
                [set_num, name, year, theme_id, num_parts])

def generate_random_part(cursor, part_num, part_cat_id):
    name = generate_random_string(20)
    insert_data(cursor, 'Parts', ['part_num', 'name', 'part_cat_id'], [part_num, name, part_cat_id])

def generate_random_inventory(cursor, inventory_id, set_num):
    version = random.randint(1, 5)
    insert_data(cursor, 'inventories', ['version', 'set_num'], [version, set_num])

def generate_random_sets_part(cursor, inventory_id, part_num, color_id):
    quantity = random.randint(1, 10)
    is_spare = random.choice([True, False])
    insert_data(cursor, 'Sets_part', ['inventory_id', 'part_num', 'color_id', 'quantity', 'is_spare'],
                [inventory_id, part_num, color_id, quantity, is_spare])

# Generowanie danych
def generate_data(n):
    theme_id = 1
    part_cat_id = 1
    color_id = 1
    set_num = 1
    inventory_id = 1
    part_num = 1

    for _ in range(n):
        generate_random_theme(cursor, theme_id)
        generate_random_part_category(cursor, part_cat_id)
        generate_random_color(cursor, color_id)
        generate_random_set(cursor, f"S{set_num:06d}", theme_id)
        generate_random_part(cursor, f"P{part_num:06d}", part_cat_id)
        generate_random_inventory(cursor, inventory_id, f"S{set_num:06d}")
        generate_random_sets_part(cursor, inventory_id, f"P{part_num:06d}", color_id)

        theme_id += 1
        part_cat_id += 1
        color_id += 1
        set_num += 1
        inventory_id += 1
        part_num += 1

    connection.commit()
    print(f"{n} rekordów zostało dodanych do bazy danych.")

# Wprowadzenie n rekordów
n = 1000  # Możesz zmienić wartość n na 10, 100, 1000 lub 5000
start_time = time.time()
generate_data(n)
end_time = time.time()

execution_time = end_time - start_time
print("Czas wykonania funkcji:", execution_time, "sekund")