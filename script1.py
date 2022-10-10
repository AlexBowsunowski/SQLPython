import psycopg2
import time
import multiprocess
import matplotlib.pyplot as plt

# config
DB_CONFIG = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "1234",
    "host": "localhost",
    "port": 5432
}
ISOLATION_LEVELS = {
        "READ_UNCOMMITTED": psycopg2.extensions.ISOLATION_LEVEL_READ_UNCOMMITTED,
        "READ_COMMITTED": psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
        "REPEATABLE_READ": psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
        "SERIALIZABLE": psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE
}
TABLE_NAME = "table_name"
TIME_DURATION_SEC = 25 


def db_connect():
    conn = psycopg2.connect(**DB_CONFIG)
    return conn


def create_table():
    query = f"""CREATE TABLE IF NOT EXISTS {TABLE_NAME}(
                id INTEGER NOT NULL 
                );
                TRUNCATE {TABLE_NAME};
    """
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(query)
        conn.commit()


def truncate_table():
    query = f"TRUNCATE {TABLE_NAME}"

    with db_connect() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query)


def count(cursor):
    query = f"""SELECT COUNT(*) FROM {TABLE_NAME}"""
    cursor.execute(query)

    time.sleep(1 - time.time() % 1)
    # time.sleep(1)

    return cursor.fetchone()[0] 


def insert(cursor):
    query = f"""INSERT INTO {TABLE_NAME} DEFAULT VALUES"""
    cursor.execute(query)


def a(isolation_level, cnt_a):
    with db_connect() as conn:
        conn.set_isolation_level(isolation_level)
        with conn.cursor() as cursor:
            for sec in range(TIME_DURATION_SEC):
                if sec % 5 == 0:
                    insert(cursor)
                    conn.commit()
                cnt_a[sec] = count(cursor)


def b(isolation_level, cnt_b):
    with db_connect() as conn:
        conn.set_isolation_level(isolation_level)
        with conn.cursor() as cursor:
            for sec in range(TIME_DURATION_SEC):
                cnt_b[sec] = count(cursor)


def execute_process(isolation_level_name, isolation_level):
    cnt_a = multiprocess.Array("i", range(TIME_DURATION_SEC))
    cnt_b = multiprocess.Array("i", range(TIME_DURATION_SEC))

    a_run = multiprocess.Process(target=a, args=(isolation_level, cnt_a))
    a_run.start()
    b_run = multiprocess.Process(target=b, args=(isolation_level, cnt_b))
    b_run.start()
    a_run.join()
    b_run.join()

    print(f"Count process A: {cnt_a[:]}")
    print(f"Count process B: {cnt_b[:]}")
    x_range = range(TIME_DURATION_SEC) 
    y_range = min(len(cnt_a), len(cnt_b))
    plt.plot(x_range, cnt_a[:y_range], label='A')
    plt.plot(x_range, cnt_b[:y_range], label='B')
    plt.xlabel('sec.')
    plt.ylabel('count')
    plt.legend()
    plt.title(isolation_level_name)
    plt.savefig(f"{isolation_level_name}.png")


if __name__ == "__main__":
    for isolation_level_name, isolation_level in ISOLATION_LEVELS.items():
        print(f"Isolation level name: {isolation_level_name}, {isolation_level}")
        create_table()
        
        execute_process(isolation_level_name, isolation_level)

        truncate_table()
        CNT_A = []
        CNT_B = []    




