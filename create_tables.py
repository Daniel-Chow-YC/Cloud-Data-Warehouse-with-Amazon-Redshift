import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description:
    Drops each table using the queries in `drop_table_queries` list.
    
    Arguments:
        cur: the cursor object
        conn: connection to the database 
        
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description:
    Creates each table using the queries in `create_table_queries` list. 
    
    Arguments:
        cur: the cursor object
        conn: connection to the database 
        
    Returns:
        None
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Description:
    Collects the required params from the config file, opens a DB connection, then drops tables (if they exist) and creates tables by runnin the two functions defined above. The DB connection is then closed.
    
    Arguments:
        None 
        
    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()