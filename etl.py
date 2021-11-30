import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    # load data from json files in the S3 bucket to the staging tables
    for count, query in enumerate(copy_table_queries):
        cur.execute(query)
        conn.commit()
        print(f"STAGING TABLE {count+1}/{len(copy_table_queries)} LOADED")


def insert_tables(cur, conn):
    for count, query in enumerate(insert_table_queries):
        cur.execute(query)
        conn.commit()
        print(f"TABLE {count+1}/{len(insert_table_queries)} FILLED")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("\n*** CONNECTING TO DB ***")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print("*** CONNECTED TO DB ***")
    
    print("\n*** LOADING STAGING TABLES ***")
    load_staging_tables(cur, conn)
    print("\n*** INSERTING INTO TABLES ***")
    insert_tables(cur, conn)

    print("\n*** CLOSING CONNECTION ***")
    conn.close()
    print("*** CONNECTION CLOSED ***\n")


if __name__ == "__main__":
    main()