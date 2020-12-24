import psycopg2
from sql_queries import create_table_queries, drop_table_queries 


def create_database():

    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()  

    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    return cur, conn

    #run the drop tables query, to drop any tables if any of it exists 
def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

    #run the create tables query, to create our tables  
def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
        
def main():
    #Exception Handling for the above functions for any errors that might occur 
    try:
        cur,conn = create_database()
    except psycopg2.Error as e: 
        print("Error: Creating a Database")
        print(e)
     
    try:
        drop_tables(cur, conn)
    except psycopg2.Error as e: 
        print("Error: Droping Tables")
        print(e)
    
    
    try:
        create_tables(cur, conn)
    except psycopg2.Error as e: 
        print("Error: Creating Tables")
        print(e)

    conn.close()


if __name__ == "__main__":
    main()