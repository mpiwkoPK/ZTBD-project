import mysql.connector
import time

# Połączenie z MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="example",
    database="lego"
)
cursor = connection.cursor()

# Definiujemy numer zestawu, który chcemy zaktualizować
set_num_to_update = '0015-1'

# Rozpoczęcie transakcji
start_time = time.time()
try:
    # Zmiana koloru wszystkich części na "green"
    update_colors_query = """
    UPDATE Colors
    SET name = 'green', rgb = '00FF00'
    WHERE ID IN (
        SELECT sp.color_id
        FROM Sets_part sp
        JOIN inventories i ON sp.inventory_id = i.ID
        WHERE i.set_num = %s
    )
    """
    cursor.execute(update_colors_query, (set_num_to_update,))

    # Aktualizacja liczby części zestawu
    update_num_parts_query = """
    UPDATE Sets
    SET num_parts = num_parts + 50
    WHERE set_num = %s
    """
    cursor.execute(update_num_parts_query, (set_num_to_update,))

    # Pobranie ID pierwszej części
    select_first_part_query = """
    SELECT sp.part_num, sp.quantity
    FROM Sets_part sp
    JOIN inventories i ON sp.inventory_id = i.ID
    WHERE i.set_num = %s
    LIMIT 1
    """
    cursor.execute(select_first_part_query, (set_num_to_update,))
    first_part = cursor.fetchone()
    if first_part:
        first_part_num, first_quantity = first_part
        # Aktualizacja ilości pierwszej części
        update_first_part_quantity_query = """
        UPDATE Sets_part
        SET quantity = %s
        WHERE part_num = %s
        AND inventory_id IN (
            SELECT ID
            FROM inventories
            WHERE set_num = %s
        )
        """
        cursor.execute(update_first_part_quantity_query, (first_quantity * 2, first_part_num, set_num_to_update))

    # Aktualizacja nazwy motywu
    update_theme_name_query = """
    UPDATE Themes
    SET name = 'Updated Classic'
    WHERE ID = (
        SELECT theme_id
        FROM Sets
        WHERE set_num = %s
    )
    """
    cursor.execute(update_theme_name_query, (set_num_to_update,))

    # Zatwierdzenie transakcji
    connection.commit()
    end_time = time.time()

    print("Updated successfully")
    print(f"Time taken: {end_time - start_time} seconds")

except Exception as e:
    # W przypadku błędu wycofanie transakcji
    connection.rollback()
    print(f"Error: {e}")

finally:
    cursor.close()
    connection.close()
