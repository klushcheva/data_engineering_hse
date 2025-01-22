from datetime import datetime
from config import Config
from scripts.py_scripts.prepare_data import *
from scripts.py_scripts.prepare_env import *
from scripts.py_scripts.process_data import archive_files, process_reports

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("app.log"),
        logging.StreamHandler()
    ]
)

data_folder = 'source'
archive_folder = 'archive'

config = Config.remote
ddl_path = Path.cwd() / 'main.ddl'

try:
    with establish_connection(config) as conn:
        execute_ddl(conn, ddl_path)

        passport_blacklist_files, terminals_files, transactions_files = get_data_files(data_folder)

        dates = [f[10:18] for f in terminals_files]
        formatted_dates = [f"{date[4:]}-{date[2:4]}-{date[:2]}" for date in dates]

        #прочитаем транзакции и причешем файл
        try:
            transactions = pd.read_csv(os.path.join(data_folder, transactions_files[1]), sep=';')
            logging.info(f"Успешно прочитали файл {transactions}")
        except Exception as e:
            logging.error(f"Ошибка чтения файла {transactions}: {e}")

        transactions["amount"] = transactions["amount"] \
            .astype(str) \
            .str.strip() \
            .str.replace(",", ".") \
            .astype(float)
        transactions.rename(columns={'transaction_id':'trans_id','transaction_date':'trans_date'})
        query = open(Path.cwd()/"scripts"/"sql_scripts"/"transactions_2stg.sql", "r").read()

        #загрузим в стейджинг
        with conn.cursor() as cursor:
            try:
                cursor.executemany(query, transactions.values.tolist())
                conn.commit()
                print("Данные 'transactions' успешно вставлены в стейджинг.")
            except Exception as e:
                conn.rollback()
                print(f"Ошибка вставки данных 'transactions' в стейджинг: {e}")

        # прочитаем терминалы и добавим даты
        terminals = pd.read_excel(os.path.join(data_folder, terminals_files[1]))
        terminals['create_dt'] = formatted_dates[1]

        #загрузим терминалы в стейджинг
        query = open(Path.cwd()/"scripts"/"sql_scripts"/"terminals_2stg.sql", "r").read()
        with conn.cursor() as cursor:
            try:
                cursor.executemany(query, terminals.values.tolist())
                conn.commit()
                print("Данные 'terminals' успешно вставлены в стейджинг.")
            except Exception as e:
                conn.rollback()
                print(f"Ошибка вставки данных 'terminals' в стейджинг: {e}")

        # прочитаем паспорта и причешем файл
        passport_blacklist = pd.read_excel(os.path.join(data_folder, passport_blacklist_files[1]))
        passport_blacklist.rename(columns={"date":"entry_dt","passport":"passport_num"})

        #щзагрузим паспорта в стейджинг
        query = open(Path.cwd()/"scripts"/"sql_scripts"/"passport_blacklist_2stg.sql", "r").read()
        with conn.cursor() as cursor:
            try:
                cursor.executemany(query, passport_blacklist.values.tolist())
                conn.commit()
                print("Данные 'passport_blacklist' успешно вставлены в стейджинг.")
            except Exception as e:
                conn.rollback()
                print(f"Ошибка вставки данных 'passport_blacklist' в стейджинг: {e}")

        # прочитаем таблицу с клиентами и загрузим в стейджинг
        try:
            with conn.cursor() as cursor:
                cursor.execute('SELECT * FROM info.clients')
                records = cursor.fetchall()
                col_names = [desc[0] for desc in cursor.description]
            clients_df = pd.DataFrame(records, columns=col_names)
            load_dataframe_to_staging(conn, clients_df, Path.cwd()/"scripts"/"sql_scripts"/"clients_2stg.sql",
                                      "clients")
        except Exception as e:
            logging.error(f"Ошибка загрузки: {e}")

        # повторим для счетов и карт
        for table in ['accounts', 'cards']:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(f'SELECT * FROM info.{table}')
                    records = cursor.fetchall()
                    col_names = [desc[0] for desc in cursor.description]
                df = pd.DataFrame(records, columns=col_names)
                load_dataframe_to_staging(conn, df, Path.cwd()/f"scripts"/f"sql_scripts"/f"{table}_2stg.sql", table)
            except Exception as e:
                logging.error(f"Ошибка загрузки: {e}")

        # загрузим всё в dwh
        dwh_scripts = [
            "terminals_stg2dwh.sql",
            "update_terminals_dwh.sql",
            "cards_stg2dwh.sql",
            "accounts_stg2dwh.sql",
            "clients_stg2dwh.sql",
            "passport_blacklist_stg2dwh.sql",
            "transactions_stg2dwh.sql"
        ]
        for script in dwh_scripts:
            try:
                execute_sql_script(conn, Path.cwd()/"scripts"/"sql_scripts"/script)
            except Exception as e:
                logging.error(f"Ошибка выполнения скрипта {script}: {e}")

        # сгенерируем таблицу с результатами
        report_dt = datetime.now().strftime('%Y-%m-%d')
        process_reports(conn, report_dt)

except Exception as e:
    logging.critical(f"Операция остановлена, ошибка: {e}")
else:
    logging.info("Все операции успешно завершены.")
finally:
    # архивируем файлы
    try:
        all_files = []

        try:
            data_files = get_data_files(data_folder)
            for file_group in data_files.values():
                if file_group:
                    all_files.append(file_group[0])
        except FileNotFoundError:
            logging.warning(f"Пропускаем несуществующий файл")
        if all_files:
            archive_files(all_files, archive_folder)
        else:
            logging.info("Файлов для архивации не обнаружено")

    except Exception as e:
        logging.error(f"Возникла ошибка при архивации: {e}")

logging.info(f'Таблица с результатами загружена.')

conn.close()