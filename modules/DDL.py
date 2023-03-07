import psycopg2
import os
import logging

from configparser import ConfigParser
from typing import Dict, Union


logging.basicConfig(level=logging.INFO)


def parse_ini() -> Union[str, Dict]:
    """
    Функция извлечения информации о пути проекта и подключении к БД из конфигурационного файла
    """
    # Создание плавающего пути к исполняемому файлу
    basedir = os.path.dirname(os.path.abspath(__file__))

    parser = ConfigParser()
    parser.read(os.path.join(basedir, 'ddl.ini'))

    path_info = parser.get('path', 'path')
    conn_info = {param[0]: param[1] for param in parser.items('postgresql')}
    return path_info, conn_info


def create_user(
        connect_info: Dict[str, str]
) -> None:
    """
    Функция создания нового пользователя и пароля
    """

    # Создание ссылки между папками (необходимо каждый раз после перезагрузки) для корректной работы в среде Unix
    os.system('ln -s /var/run/postgresql/.s.PGSQL.5432 /tmp/.s.PGSQL.5432')

    # Создание нового пользователя и пароля
    logging.info('\nCreate new user')
    os.system(f"sudo -Su postgres createuser -sl {connect_info['user']}")
    logging.info('\nCreate user password')
    os.system(f'''sudo -S -iu postgres psql -c \
            "ALTER USER {connect_info['user']} WITH PASSWORD '{connect_info['password']}';"''')


def create_connection(
        connect_info: Dict[str, str]
) -> psycopg2.extensions.connection:
    """
    Функция создания подключения к БД
    """

    conn = None
    try:
        conn = psycopg2.connect(**connect_info)
        logging.info(f" * SUCCESS *: Connection to PostgreSQL (user:{connect_info['user']}, "
                     f"database:{connect_info['database']}) complete.")
    except Exception as e:
        logging.error(f"{type(e).__name__}: '{e}' occurred.")
    return conn


def create_db(
        connect_info: Dict[str, str]
) -> None:
    """
    Функция создания БД
    """

    # Создание нового пользователя и присвоение пароля, используя данные из конфигурационного файла
    create_user(connect_info)

    # Создание соединения с БД, используя данные из конфигурационного файла
    psql_connection_dict = {
        'database': 'template1',
        'user': f'{connect_info["user"]}',
        'password': f'{connect_info["password"]}'
    }
    conn = create_connection(psql_connection_dict)
    cur = conn.cursor()

    # Создание базы данных
    conn.autocommit = True
    sql_query = f"CREATE DATABASE {connect_info['database']}"

    try:
        cur.execute(sql_query)
        logging.info(f" * SUCCESS *: Create database \'{connect_info['database']}\' complete.")
    except Exception as e:
        logging.error(f"{type(e).__name__}: {e} ")
        cur.close()
    else:
        conn.autocommit = False


def execute_query(
        sql_query: str,
        conn: psycopg2.extensions.connection,
        cur: psycopg2.extensions.cursor
) -> None:
    """
    Функция выполнения SQL запроса
    """

    conn.autocommit = True
    try:
        cur.execute(sql_query)
        logging.info(f" * SUCCESS *: Query \'{sql_query[9:30]}...\' executed.")
    except Exception as e:
        logging.error(f"{type(e).__name__}: {e} ")
    else:
        conn.autocommit = False


def ddl():
    """
    Главная функция
    """

    logging.info('\n-------------------Create PostgreSQL database-------------------\n')
    # Считывание данных из конфигурационного файла
    path_info, conn_info = parse_ini()

    # Создание базы данных
    create_db(conn_info)

    conn = create_connection(conn_info)
    cursor = conn.cursor()

    # Создание таблицы db_sessions
    db_sessions_sql = '''
        CREATE TABLE IF NOT EXISTS db_sessions (
           session_id VARCHAR(50) NOT NULL UNIQUE PRIMARY KEY, 
           client_id VARCHAR(50) NOT NULL,
           visit_date DATE,
           visit_time TIME NOT NULL,
           visit_number SMALLINT NOT NULL, 
           utm_source VARCHAR(50) NOT NULL,
           utm_medium VARCHAR(50) NOT NULL,
           utm_campaign VARCHAR(50) NOT NULL,
           utm_adcontent VARCHAR(50) NOT NULL,
           device_category VARCHAR(50) NOT NULL,
           device_brand VARCHAR(50) NOT NULL,
           device_screen_resolution VARCHAR(50) NOT NULL,
           device_browser VARCHAR(50) NOT NULL,
           geo_country VARCHAR(50) NOT NULL,
           geo_city VARCHAR(50) NOT NULL
            )
    '''

    execute_query(db_sessions_sql, conn, cursor)

    # Создание таблицы db_hits
    db_hits_sql = '''
        CREATE TABLE IF NOT EXISTS db_hits (
            session_id VARCHAR(50) NOT NULL,
            hit_date DATE NOT NULL,
            hit_number SMALLINT NOT NULL,
            hit_page_path TEXT NOT NULL,
            event_category VARCHAR(50) NOT NULL,
            event_action VARCHAR(50) NOT NULL,
            PRIMARY KEY (session_id, hit_number)
            )            
            
    '''

    execute_query(db_hits_sql, conn, cursor)

    # Импорт обработанных данных из csv в таблицу db_sessions
    path_to_sessions = f'{path_info}/data/prep_data/ga_sessions_prep.csv'
    ga_sessions_prep_sql = f'''
        COPY db_sessions FROM '{path_to_sessions}' HEADER CSV
    '''
    execute_query(ga_sessions_prep_sql, conn, cursor)

    # Импорт обработанных данных из csv в таблицу db_hits
    path_to_hits = f'{path_info}/data/prep_data/ga_hits_prep.csv'
    ga_hits_prep_sql = f'''
        COPY db_hits FROM '{path_to_hits}' HEADER CSV
    '''
    execute_query(ga_hits_prep_sql, conn, cursor)

    # Удаление строк в таблице db_hits с значениями session_id, которых нет в таблице db_sessions
    db_delete_absent_sql = f'''
        DELETE FROM db_hits h WHERE NOT EXISTS (SELECT 1 FROM db_sessions s WHERE h.session_id = s.session_id)
    '''
    execute_query(db_delete_absent_sql, conn, cursor)

    # Создание внешнего ключа в таблице db_hits
    fr_key_sql = f'''
        ALTER TABLE db_hits ADD FOREIGN KEY(session_id) REFERENCES db_sessions(session_id)
    '''
    execute_query(fr_key_sql, conn, cursor)

    conn.close()
    cursor.close()

    # Удаление временных файлов
    if os.path.isfile(path_to_sessions):
        os.remove(path_to_sessions)
        logging.info(f" * SUCCESS *: Delete temporary file \'{path_to_sessions.split('/')[-1]}\'.")
    else:
        logging.warning(f"File \'{path_to_sessions.split('/')[-1]}\' doesn't exists.")

    if os.path.isfile(path_to_hits):
        os.remove(path_to_hits)
        logging.info(f" * SUCCESS *: Delete temporary file \'{path_to_hits.split('/')[-1]}\'.")
    else:
        logging.warning(f"File \'{path_to_hits.split('/')[-1]}\' doesn't exists.")

    
if __name__ == "__main__":
    ddl()
