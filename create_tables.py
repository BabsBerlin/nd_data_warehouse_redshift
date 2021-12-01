import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    for count, query in enumerate(drop_table_queries):
        cur.execute(query)
        conn.commit()
        print(f"TABLE {count+1}/{len(drop_table_queries)} DROPPED")


def create_tables(cur, conn):
    for count, query in enumerate(create_table_queries):
        cur.execute(query)
        conn.commit()
        print(f"TABLE {count+1}/{len(create_table_queries)} CREATED")


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    print("\n*** CONNECTING TO DB ***")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER']
                                    .values()))
    cur = conn.cursor()
    print("*** CONNECTED TO DB ***")

    print("\n*** DROPPING EXISTING TABLES ***")
    drop_tables(cur, conn)
    print("\n*** CREATING NEW TABLES ***")
    create_tables(cur, conn)

    print("\n*** CLOSING CONNECTION ***")
    conn.close()
    print("*** CONNECTION CLOSED ***\n")


if __name__ == "__main__":
    main()
