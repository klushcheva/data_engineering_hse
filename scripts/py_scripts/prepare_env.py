import logging
import psycopg2
from psycopg2 import sql


def establish_connection(config):
    try:
        conn = psycopg2.connect(**config)
        conn.autocommit = False
        logging.info("Установлено соединение с БД.")
        return conn
    except psycopg2.Error as e:
        logging.error(f"Не установлено соединение: {e}")
        raise


def truncate_staging_tables(conn):
    stg_tables = [
        'klus_stg_transactions',
        'klus_stg_terminals',
        'klus_stg_blacklist',
        'klus_stg_cards',
        'klus_stg_accounts',
        'klus_stg_clients'
    ]
    with conn.cursor() as cursor:
        for table in stg_tables:
            try:
                cursor.execute(sql.SQL("TRUNCATE TABLE {}").format(sql.Identifier(table)))
                logging.info(f"Очищена таблица: {table}")
            except Exception as e:
                logging.error(f"Ошибка в очистке таблицы {table}: {e}")
    conn.commit()
    logging.info("Стейджинг очищен.")


def execute_ddl(conn, ddl_path):
    truncate_staging_tables(conn)
    try:
        with ddl_path.open('r') as file:
            ddl_commands = file.read()
        with conn.cursor() as cursor:
            cursor.execute(ddl_commands)
        conn.commit()
        logging.info("Таблицы из main.ddl созданы")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка в создании таблиц: {e}")
        raise
