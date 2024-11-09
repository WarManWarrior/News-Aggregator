import psycopg2

conn = psycopg2.connect(
        dbname="Aggre",
        user="postgres",
        password="siddh2003",
        host="localhost",
        port="5432"
    )


idk = None

cursor = conn.cursor()

