import psycopg2
import csv
import time
import random

# Funkcja do tworzenia tabeli
def create_table(cursor):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS LegoParts (
            id SERIAL PRIMARY KEY,
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
    print("Tabela LegoPart została utworzona lub już istnieje.")

# Funkcja do wstawiania danych z pliku CSV do tabeli
def insert_data_from_csv(cursor, batch_size):
    try:
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

        print(f"{batch_size} danych z pliku CSV zostało wstawionych do tabeli pomyślnie.")

    except psycopg2.Error as error:
        print("Błąd podczas wstawiania danych z pliku CSV:", error)

# Połączenie z bazą danych PostgreSQL
try:
    connection = psycopg2.connect(
        host="localhost",
        user="postgres",
        password="example",
    )

    # Tworzenie tabeli, jeśli nie istnieje
    with connection.cursor() as cursor:
        create_table(cursor)

    # Wstawianie danych dla różnych partii
    batch_sizes = [10, 100, 1000, 5000]
    for batch_size in batch_sizes:
        start_time = time.time()
        with connection.cursor() as cursor:
            insert_data_from_csv(cursor, batch_size)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Czas wykonania dla partii o rozmiarze {batch_size}: {execution_time} sekund")

except psycopg2.Error as error:
    print("Błąd podczas połączenia z bazą danych PostgreSQL:", error)

finally:
    if 'connection' in locals():
        connection.close()