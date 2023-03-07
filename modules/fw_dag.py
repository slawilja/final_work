import datetime as dt
import os
import sys
import psycopg2
import logging
import glob
import json
import pandas as pd

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from configparser import ConfigParser
from typing import Dict, Union
from datetime import datetime
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sqlalchemy import create_engine


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


def preprocessing() -> None:
    """
    Функция обработки данных в json файлах
    """

    def file_to_df(
            file_path: str
    ) -> pd.DataFrame:
        """
        Функция загрузки из json в pandas.dataframe
        """

        with open(file_path, 'r') as j:
            j_data = json.load(j)
        file_date = list(j_data.keys())[0]
        if len(j_data[file_date]) < 1:
            logging.warning(f" Data \'{file_path.split('/')[-1]}\' is empty.\n")
            df = None
        else:
            df = pd.DataFrame(j_data[file_date])
            logging.info(f" * SUCCESS *: Read file \'{file_path.split('/')[-1]}\' complete.")
        return df


    def save_to_csv(
            df: pd.DataFrame,
            file_name: str
    ) -> None:
        """
        Фуекция сохранения датафрейма в
        :param df:
        :param file_name:
        :return:
        """
        try:
            df.to_csv(f'{path_info}/data/prep_data/{file_name}.csv', index=False)
            logging.info(f" * SUCCESS *: Save data \'{file_name}.csv\' to \'{path_info}/data/prep_data\' complete.")
        except Exception as e:
            logging.error(f"{type(e).__name__}: '{e}' occurred.")


    def filter_data_hits(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Функция удаления колонок в hits с процентом пропущенных значений более 20
        """

        columns_to_drop = [
            'event_value',
            'hit_time',
            'hit_referer',
            'event_label',
            'hit_type'
        ]
        df = df.drop(columns_to_drop, axis=1)
        return df


    def filter_data_sessions(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Функция удаления колонок в sessions с процентом пропущенных значений более 20
        """

        columns_to_drop = [
            'device_model',
            'utm_keyword',
            'device_os'
        ]
        df = df.drop(columns_to_drop, axis=1)
        return df


    def corr_types_hits(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Функция приведения типов в hits
        """

        df['hit_number'] = df['hit_number'].astype('int64')
        df['hit_date'] = pd.to_datetime(df.hit_date)
        obj_types = df.dtypes[df.dtypes == 'object'].index.to_list()
        df[obj_types] = df[obj_types].astype('str')
        return df


    def corr_types_sessions(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Функция приведения типов в sessions
        """

        df['visit_number'] = df['visit_number'].astype('int64')
        df['visit_date'] = pd.to_datetime(df['visit_date'])
        df['visit_time'] = df['visit_time'].apply(lambda x: datetime.strptime(x, '%H:%M:%S').time())
        obj_types = df.dtypes[df.dtypes == 'object'].index.to_list()
        df[obj_types] = df[obj_types].astype('str')
        return df


    def del_na_hits(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Функция удаляющие пропуски в числовых колонках в hits
        """

        col = [
            'hit_date',
            'hit_number',
            'session_id'
        ]
        for i in col:
            df = df[df[i].notna()]
        return df


    def del_na_sessions(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Функция удаляющие пропуски в числовых колонках в sessions
        """

        col = [
            'visit_number',
            'visit_time',
            'visit_date',
            'session_id',
            'client_id'
        ]
        for i in col:
            df = df[df[i].notna()]
        return df


    def fill_cat_col_hits(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Функция заполнения пропусков категориальных признаков в hits
        """

        col = [
            'hit_page_path',
            'event_category',
            'event_action'
        ]
        for i in col:
            df[i] = df[i].fillna('other')
        return df


    def fill_cat_col_sessions(
            df: pd.DataFrame
    ) -> pd.DataFrame:
        """
        Функция заполнения пропусков категориальных признаков в sessions
        """

        col = [
            'utm_source',
            'utm_medium',
            'utm_campaign',
            'utm_adcontent',
            'device_category',
            'device_brand',
            'device_screen_resolution',
            'device_browser',
            'geo_country',
            'geo_city'
        ]
        for i in col:
            df[i] = df[i].fillna('other')
        return df

    # Конвейер обработки hits
    preprocessor_hits = Pipeline([
        ('filter_hits', FunctionTransformer(filter_data_hits)),
        ('fill_cat_columns_hits', FunctionTransformer(fill_cat_col_hits)),
        ('del_na_hits', FunctionTransformer(del_na_hits)),
        ('types_hits', FunctionTransformer(corr_types_hits))
    ])

    # Конвейер обработки sessions
    preprocessor_sessions = Pipeline([
        ('filter_sessions', FunctionTransformer(filter_data_sessions)),
        ('fill_cat_columns_sessions', FunctionTransformer(fill_cat_col_sessions)),
        ('del_na_sessions', FunctionTransformer(del_na_sessions)),
        ('types_sessions', FunctionTransformer(corr_types_sessions))
    ])

    # Создание списка имен файлов вместе с путями
    extra_files = glob.glob(f'{path_info}/data/extra_data/*.json')

    # Обработка и сохрание в csv
    for file in extra_files:
        df = file_to_df(file)
        if df is not None:
            if 'sessions' in file:
                pipe_sessions = preprocessor_sessions.fit_transform(df)
                save_to_csv(pipe_sessions, f"prep_{file.split('/')[-1].split('.')[0]}")
            if 'hits' in file:
                pipe_hits = preprocessor_hits.fit_transform(df)
                save_to_csv(pipe_hits, f"prep_{file.split('/')[-1].split('.')[0]}")


def add_data():
    """
    Функция импорта обработанных данных в БД
    """

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


    def insert_into_table(
            df: pd.DataFrame,
            table: str,
            file: str,
            cur: psycopg2.extensions.cursor,
            conn: psycopg2.extensions.connection
    ) -> None:
        """
        Функция импорта датафрейма в БД
        """

        values = "VALUES({})".format(",".join(["%s" for _ in df.columns]))

        # Создание строки из имен колонок, разделенных запятой
        cols = ','.join(list(df.columns))

        # SQL запрос на добавление в БД
        query = f"INSERT INTO {table} ({cols}) {values} ON CONFLICT DO NOTHING"

        try:
            cur.executemany(query, df.values)
            conn.commit()
            logging.info(f" * SUCCESS *: Add data from {file.split('/')[-1]} complete.\n")

        except Exception as e:
            logging.error(f"{type(e).__name__}: {e} ")
            conn.rollback()

    engine = create_engine(f"postgresql+psycopg2://{conn_info['user']}:{conn_info['password']}@{conn_info['host']}" +
                           f":{conn_info['port']}/{conn_info['database']}")
    conn = create_connection(conn_info)
    cur = conn.cursor()

    # Создание списков имен файлов вместе с путями
    extra_files = glob.glob(f'{path}/data/prep_data/prep*.csv')
    files_session = [x for x in extra_files if 'session' in x]
    files_hits = [x for x in extra_files if 'hits' in x]

    # Создание отсортированного списка дат из имен файлов
    dates_files = sorted(list({x.split('/')[-1].split('_')[-1].split('.')[0] for x in extra_files}))

    # Импорт в БД файлов sessions
    for date in dates_files:
        for file in files_session:
            if date in file:
                df = pd.read_csv(file)
                if df is not None:
                    insert_into_table(df, 'db_sessions', file, cur, conn)

    # Создание списка session_id из таблицы db_sessions в БД
    columns = list(pd.read_sql('SELECT session_id FROM db_sessions', con=engine).to_dict()['session_id'].values())

    # Импорт в БД файлов hits
    for date in dates_files:
        for file in files_hits:
            if date in file:
                df = pd.read_csv(file)
                if df is not None:
                    # Удаление строк, у которых session_id отсутствует в таблице db_sessions
                    df = df[df.session_id.isin(columns)]

                    insert_into_table(df, 'db_hits', file, cur, conn)

    # Удаление временных файлов
    for file in extra_files:
        if os.path.isfile(file):
            os.remove(file)
            logging.info(f" * SUCCESS *: Delete file \'{file.split('/')[-1]}\'.")
        else:
            logging.warning(f"File \'{file.split('/')[-1]}\' doesn't exists.")


# path = os.path.expanduser('~/airflow_hw')
path_info, conn_info = parse_ini()
path = path_info
# Добавление пути к коду проекта в переменную окружения, чтобы он был доступен python-процессу
os.environ['PROJECT_PATH'] = path
# Добавление пути к коду проекта в $PATH, чтобы импортировать функции
sys.path.insert(0, path)


args = {
    'owner': 'airflow',
    'start_date': dt.datetime(2023, 3, 7),
    'retries': 1,
    'retry_delay': dt.timedelta(minutes=3),
    'depends_on_past': False,
}

with DAG(
        dag_id='add_extra_data',
        schedule="00 15 * * *",
        default_args=args,
) as dag:
    preprocessing = PythonOperator(
        task_id='preprocessing',
        python_callable=preprocessing,
        dag=dag
    )

    add_data = PythonOperator(
        task_id='add_data_to_database',
        python_callable=add_data,
        dag=dag
    )

    preprocessing >> add_data
