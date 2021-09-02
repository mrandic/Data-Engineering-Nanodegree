import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_database():
    """
    - Creates and connects to the udacitydb
    @return: cursor and connection to udacitydb
    """

    # connect to default local database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create udacitydb database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS udacitydb")
    cur.execute("CREATE DATABASE udacitydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to udacitydb database
    conn = psycopg2.connect("host=127.0.0.1 dbname=udacitydb user=student password=student")
    cur = conn.cursor()

    return cur, conn


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list.
    @param cur:
    @param conn:
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    @param cur:
    @param conn:
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and creates the udacitydb database.
    - Establishes connection with the udacitydb database and gets
    cursor to it.
    - Function drops all the tables.
    - Function creates all tables needed.
    - In the end, connection gets closed.
    """
    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()