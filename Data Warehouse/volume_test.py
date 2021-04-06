import psycopg2
import configparser
from sql_queries import volume_test_queries


def volume_test(cur, conn):
    """
    Run the queries in the 'volume_test_queries' list on staging and dimensional tables.
    :param cur: cursor object to database connection
    :param conn: connection object to database
    """
    
    for query in volume_test_queries:
        print('Running ' + query)         
        try:
            cur.execute(query)
            results = cur.fetchone()

            for row in results:
                print("   ", row)
                conn.commit()
                
        except psycopg2.Error as e:
            print(e)
            conn.close()


def main():
    """
    Run COUNT(*) query on the staging and dimensional tables to validate that the data has been loaded into Redshift
    """
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    volume_test(cur, conn)
    conn.close()


if __name__ == "__main__":
    main()