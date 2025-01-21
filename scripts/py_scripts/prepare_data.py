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

    return {
        "passport_blacklist": passport_blacklist_files,
        "terminals": terminals_files,
        "transactions": transactions_files
    }


def parse_dates(files):
    dates = []
    for file in files:
        date_str = file.stem.split('_')[-1]
        if len(date_str) == 8:
            formatted_date = f"{date_str[4:]}-{date_str[2:4]}-{date_str[:2]}"
            dates.append(formatted_date)
        else:
            logging.warning(f"В названии файла {file.name} нет валидной даты.")
            dates.append(None)
    return dates


def load_csv_to_staging(conn, file_path, query_path, preprocess=None):
    data_path = '/Users/klushcheva/PycharmProjects/DE_HSE_project/source/'+f'{file_path}'
    try:
        df = pd.read_csv(data_path, sep=';', encoding='utf-8')
        if preprocess:
            df = preprocess(df)
        query = query_path.read()
        with conn.cursor() as cursor:
            cursor.executemany(query, df.values.tolist())
            conn.commit()
        logging.info(f"Данные из файла {file_path} успешно перенесены в стейджинг.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при перенесении данных из {file_path} в стейджинг: {e}")
        raise


def load_excel_to_staging(conn, file_path, query_path, additional_columns=None):
    try:
        df = pd.read_excel(file_path)
        if additional_columns:
            for col, value in additional_columns.items():
                df[col] = value
        query = query_path.read()
        with conn.cursor() as cursor:
            cursor.executemany(query, df.values.tolist())
            conn.commit()
        logging.info(f"Данные из файла {file_path} успешно перенесены в стейджинг.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Ошибка при перенесении данных из {file_path} в стейджинг: {e}")
        raise


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