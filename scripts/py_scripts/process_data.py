import logging
from pathlib import Path

from scripts.py_scripts.prepare_data import execute_sql_script


def process_reports(conn, report_dt):
    try:
        script_path = Path.cwd()/"scripts"/"sql_scripts"/"determine_fraud.sql"
        execute_sql_script(conn, script_path, params=(report_dt,))
        logging.info("Отчеты обработаны.")
    except Exception as e:
        logging.error(f"Ошибка в обработке отчетов: {e}")


def archive_files(files, archive_folder):
    archive_path = Path(archive_folder)
    archive_path.mkdir(exist_ok=True)
    for file in files:
        try:
            destination = archive_path/f"{file.name}.backup"
            file.rename(destination)
            logging.info(f"Файл {file.name} заархивирован в папку {destination}.")
        except Exception as e:
            logging.error(f"Ошибка при архивации {file.name}: {e}")