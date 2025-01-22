import logging
import os
from pathlib import Path

import pandas as pd


def get_data_files(data_folder):
    data_path = Path.cwd()/data_folder
    if not data_path.exists() or not data_path.is_dir():
        logging.error(f"Папки '{data_folder}' не существует.")
        raise FileNotFoundError

    files = os.listdir(data_path)
    passport_blacklist_files = sorted([f for f in files if f.startswith("passport_blacklist_")])
    terminals_files = sorted([f for f in files if f.startswith("terminals_")])
    transactions_files = sorted([f for f in files if f.startswith("transactions_")])

    return passport_blacklist_files, terminals_files, transactions_files

def load_dataframe_to_staging(conn, df, query_path, table_name):
    try:
        query = query_path.read_text()
        with conn.cursor() as cursor:
            cursor.executemany(query, df.values.tolist())
        conn.commit()
        logging.info(f"Данные из датафрейма '{table_name}' успешно перенесены в стейджинг.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при перенесении датафрейма '{table_name}' в стейджинг: {e}")
        raise


def execute_sql_script(conn, script_path, params=None):
    try:
        script = script_path.read_text()
        with conn.cursor() as cursor:
            if params:
                for command in filter(None, map(str.strip, script.split(';'))):
                    cursor.execute(command, params)
            else:
                cursor.execute(script)
        conn.commit()
        logging.info(f"Выполнен скрипт: {script_path.name}")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при выполнении скрипта {script_path.name}: {e}")
        raise