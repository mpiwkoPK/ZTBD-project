import mysql.connector
import csv
import time
import random

#plik do wstawiania 10,100,1000 rekordów bez relacji

def create_database(cursor, database_name):
    query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    cursor.execute(query)
    print(f"Baza danych {database_name} została utworzona lub już istnieje.")

# Połączenie z bazą danych MySQL w kontenerze Docker
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="example"
)

# Tworzenie bazy danych, jeśli nie istnieje
create_database(connection.cursor(), 'lego_part')

# Połączenie z bazą danych 'lego_without_relation'
connection.database = 'lego_part'

# Funkcja do tworzenia tabeli
def create_table():
    cursor = connection.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS LegoParts (
            id INT AUTO_INCREMENT PRIMARY KEY,
            version INT,
            set_num VARCHAR(255),
            part_num VARCHAR(255),
            quantity INT,
            is_spare VARCHAR(1),
            part_name VARCHAR(255),
            part_category_name VARCHAR(255),
            colors_name VARCHAR(255),
            rgb VARCHAR(255),
            is_trans VARCHAR(1),
            sets_name VARCHAR(255),
            year INT,
            num_parts INT,
            themes_name VARCHAR(255)
        )
    """
    cursor.execute(create_table_query)
    connection.commit()
    print("Tabela LegoPart została utworzona lub już istnieje.")

# Funkcja do wstawiania danych z pliku CSV do tabeli
def insert_data_from_csv(batch_size):
    try:
        cursor = connection.cursor()

        # Wstawianie danych z pliku CSV do tabeli
        with open('dane/polaczone_dane.csv', 'r', encoding='utf-8') as file: 
            csv_reader = csv.reader(file)
            next(csv_reader)  # Pominięcie nagłówka w pliku CSV
            rows = [row for row in csv_reader]

            for i in range(batch_size):
                row = random.choice(rows)  # Wybierz losowy wiersz z pliku CSV
                insert_query = """
                INSERT INTO LegoParts (
                    version, set_num, part_num, quantity, is_spare,
                    part_name, part_category_name, colors_name, rgb,
                    is_trans, sets_name, year, num_parts, themes_name
                ) VALUES (
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s
                )
                """
                cursor.execute(insert_query, row)

        connection.commit()
        print(f"{batch_size} danych z pliku CSV zostało wstawionych do tabeli pomyślnie.")

    except mysql.connector.Error as error:
        print("Błąd podczas wstawiania danych z pliku CSV:", error)

    finally:
        cursor.close()

batch_sizes = [10, 100, 1000, 5000]

create_table()
for batch_size in batch_sizes:
    start_time = time.time()
    insert_data_from_csv(batch_size)
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Czas wykonania dla partii o rozmiarze {batch_size}: {execution_time} sekund")

connection.close()