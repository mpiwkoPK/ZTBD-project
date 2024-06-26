import mysql.connector
import csv
import time

def create_database(cursor, database_name):
    query = f"CREATE DATABASE IF NOT EXISTS {database_name}"
    cursor.execute(query)
    print(f"Baza danych {database_name} została utworzona lub już istnieje.")

# Połączenie z bazą danych MySQL w kontenerze Docker
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="example",
    port="3306"  # Port MySQL w kontenerze
)

# Tworzenie bazy danych, jeśli nie istnieje
create_database(connection.cursor(), 'lego_without_relation')

# Połączenie z bazą danych 'lego_without_relation'
connection.database = 'lego_without_relation'

#Funkcja do tworzenia tabeli
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
    print("Tabela LegoParts została utworzona lub już istnieje.")

# Funkcja do wstawiania danych z pliku CSV do tabeli
def insert_data_from_csv():
    try:
        cursor = connection.cursor()

        # Wstawianie danych z pliku CSV do tabeli
        with open('dane/polaczone_dane.csv', 'r', encoding='utf-8') as file:  # Otwarcie pliku z kodowaniem UTF-8
            csv_reader = csv.reader(file)
            next(csv_reader)  # Pominięcie nagłówka w pliku CSV
            for row in csv_reader:
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
        print("Dane z pliku CSV zostały wstawione do tabeli pomyślnie.")

    except mysql.connector.Error as error:
        print("Błąd podczas wstawiania danych z pliku CSV:", error)

    finally:
        cursor.close()

start_time = time.time()

create_table()
insert_data_from_csv()

end_time = time.time()
execution_time = end_time - start_time

print("Czas wykonania funkcji:", execution_time, "sekund")

connection.close()