import mysql.connector

def drop_all_tables(cursor):
    # Wyłączenie sprawdzania kluczy obcych
    cursor.execute("SET FOREIGN_KEY_CHECKS = 0")

    # Pobierz listę wszystkich tabel w bieżącej bazie danych
    cursor.execute("SHOW TABLES")
    tables = cursor.fetchall()

    # Usuń każdą tabelę
    for table in tables:
        cursor.execute(f"DROP TABLE IF EXISTS {table[0]}")
        print(f"Usunięto tabelę {table[0]}")

    # Włączenie sprawdzania kluczy obcych
    cursor.execute("SET FOREIGN_KEY_CHECKS = 1")

# Połączenie z bazą danych MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="example",
    database="lego"  # Nazwa bazy danych, którą chcesz wyczyścić
)
cursor = connection.cursor()

# Wyczyszczenie bazy danych
drop_all_tables(cursor)

# Zatwierdzenie zmian
connection.commit()

# Zamknięcie połączenia
cursor.close()
connection.close()
