import concurrent.futures
from random import randint, sample

import matplotlib.pyplot as plt
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.errors import SerializationFailure

DB_CONFIG = {
    "dbname": "postgres", 
    "user":"postgres", 
    "password":'1234', 
    "host":'localhost', 
    "port": 5432
}



TABLE_NAME = "concurrent_upd"
ROWS = 1000 
PROCESSES = 20
ITERS = 4000

ISOLATION_LEVEL = psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ


def db_connect():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def insert_table(cursor):
    insert_query = sql.SQL(
            """INSERT INTO {table_name} VALUES (%s, %s)"""
        ).format(table_name=sql.Identifier(TABLE_NAME))
    
    for row in range(ROWS):
        
        insert_value = (row, "".join(sample('abcdefghijklnoprst', 6)))
        cursor.execute(insert_query, insert_value)


def create_table():
    query = sql.SQL(
        """
        CREATE TABLE IF NOT EXISTS {table_name} (
            int INTEGER PRIMARY KEY,
            text TEXT NULL
        );
        TRUNCATE {table_name};
        """     
    ).format(table_name=sql.Identifier(TABLE_NAME))

    with db_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)
            insert_table(cursor)
        conn.commit()


def truncate_table():
    query = sql.SQL("""TRUNCATE {table_name}""") \
                        .format(table_name=sql.Identifier(TABLE_NAME))

    with db_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)    


def update_table(cursor):
    change_id = randint(1, ROWS)
    new_text = "".join(sample('abcdefghijklnoprst', 6))

    update_query = sql.SQL("UPDATE {table_name} SET {set_attr} = %s WHERE {filter_attr} = %s").format(
                    table_name=sql.Identifier(TABLE_NAME),
                    set_attr=sql.Identifier('text'),
                    filter_attr=sql.Identifier('int')
                )
    
    cursor.execute(update_query, (new_text, change_id))


def concurrent_update():


    hits, fails, last_rows = 0, 0, 0

    with db_connect() as conn:
        conn.set_isolation_level(ISOLATION_LEVEL)
        with conn.cursor() as cursor:
            flag = False
            for _ in range(ITERS):
                try:
                    update_table(cursor)
                    hits += 1
                    last_rows += 1

                except SerializationFailure:
                    fails += 1
                    flag = True 

                if flag:
                    last_rows = 0
                    flag = False 

                conn.commit()
    return hits, fails, last_rows 


def execute():
    create_table()

    all_hits, all_fails, all_last_rows, pid = [], [], [], range(PROCESSES)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        processes = [
            executor.submit(concurrent_update) for _ in range(PROCESSES)
        ]
        for process in processes:
            hits, fails, last_rows = process.result()
            all_hits.append(hits)
            all_fails.append(fails)
            all_last_rows.append(last_rows)

       
    
    result = {
        "PID": pid,
        "n_hits": all_hits,
        "n_fails": all_fails,
        "n_last_rows": all_last_rows
    }

    truncate_table() 

    return pd.DataFrame.from_dict(result)


if __name__ == "__main__":
    df = execute()

    print(df.to_string(index=False))

    df.hist(column='n_last_rows')
    plt.show()
    
