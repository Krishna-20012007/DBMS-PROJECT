import psycopg2

def get_connection():
    return psycopg2.connect(
        host="localhost",
        database="television_management",
        user="postgres",
        password="Nila@125"
    
    )
