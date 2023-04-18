import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description:
    Loads the data from S3 into staging tables using the queries in `copy_table_queries` list.
    
    Arguments:
        cur: the cursor object
        conn: connection to the database 
        
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description:
    Loads the data from staging tables into analytics tables using the queries in `insert_table_queries` list.
    
    Arguments:
        cur: the cursor object
        conn: connection to the database 
        
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Description:
        Collects the required params from the config file and opens a DB connection. Then loads data from S3 to staging tables. Then loads data from staging tables to analytics tables. (By running the two functions defined above.) The DB connection is then closed.

        Arguments:
            None 

        Returns:
            None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()