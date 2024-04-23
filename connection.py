from pymongo import MongoClient
#from mysql import connector
import mysql.connector
import psycopg2

def check_mongodb_connection():
    try:
        client = MongoClient('mongodb://localhost:27017/')
        db = client.admin
        server_status_result = db.command("serverStatus")
        print("Connection to MongoDB successful.")
    except Exception as e:
        print("Error connecting to MongoDB:", e)

def check_mysql_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="example",
            database="information_schema"
        )
        if connection.is_connected():
            print("Connection to MySQL successful.")
    except Exception as e:
        print("Error connecting to MySQL:", e)

def check_postgresql_connection():
    try:
        connection = psycopg2.connect(
            host="localhost",
            user="postgres",
            password="example",
            port="5432",
            database="postgres"
        )
        cursor = connection.cursor()
        cursor.execute("SELECT version();")
        print("Connection to PostgreSQL successful.")
    except (Exception, psycopg2.Error) as error:
        print("Error connecting to PostgreSQL:", error)

if __name__ == "__main__":
    check_mongodb_connection()
    check_mysql_connection()
    check_postgresql_connection()