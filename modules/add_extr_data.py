import psycopg2
import os
import logging
import glob
import json
import pandas as pd

from configparser import ConfigParser
from typing import Dict
from datetime import datetime
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import FunctionTransformer
from sqlalchemy import create_engine

from modules.DDL import parse_ini, create_connection, execute_query
from modules.preparation import (
    prep_sessions, prep_hits, filter_data_hits, filter_data_sessions, corr_types_hits,
    corr_types_sessions, fill_cat_col_hits, fill_cat_col_sessions
)


path_info, conn_info = parse_ini()
path = os.environ.get('PROJECT_PATH', path_info)


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

    # Создание строки колонок, разделенных запятой
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


def pipeline():
    """
    Главная функция
    """

    logging.info('\n-------------------Add new/extra data-------------------\n')

    # Конвейер обработки файлов hits
    preprocessor_hits = Pipeline([
        ('filter_hits', FunctionTransformer(filter_data_hits)),
        ('fill_cat_columns_hits', FunctionTransformer(fill_cat_col_hits)),
        ('del_na_hits', FunctionTransformer(del_na_hits)),
        ('types_hits', FunctionTransformer(corr_types_hits))
    ])

    # Конвейер обработки файлов sessions
    preprocessor_sessions = Pipeline([
        ('filter_sessions', FunctionTransformer(filter_data_sessions)),
        ('fill_cat_columns_sessions', FunctionTransformer(fill_cat_col_sessions)),
        ('del_na_sessions', FunctionTransformer(del_na_sessions)),
        ('types_sessions', FunctionTransformer(corr_types_sessions))
    ])

    # Создание списков имен файлов вместе с путями
    extra_files = glob.glob(f'{path}/data/extra_data/*.json')
    files_session = [x for x in extra_files if 'session' in x]
    files_hits = [x for x in extra_files if 'hits' in x]

    # Создание отсортированного списка дат из имен файлов
    dates_files = sorted(list({x.split('/')[-1].split('_')[-1].split('.')[0] for x in extra_files}))

    engine = create_engine(f"postgresql+psycopg2://{conn_info['user']}:{conn_info['password']}@{conn_info['host']}"+
                           f":{conn_info['port']}/{conn_info['database']}")

    conn = create_connection(conn_info)
    cur = conn.cursor()

    # Обработка и импорт в БД файлов sessions
    for date in dates_files:
        for file in files_session:
            if date in file:
                df = file_to_df(file)
                if df is not None:
                    pipe_sessions = preprocessor_sessions.fit_transform(df)
                    insert_into_table(pipe_sessions, 'db_sessions', file, cur, conn)

    # Создание списка session_id из таблицы db_sessions в БД
    columns = list(pd.read_sql('SELECT session_id FROM db_sessions', con=engine).to_dict()['session_id'].values())

    # Обработка и импорт в БД файлов hits
    for date in dates_files:
        for file in files_hits:
            if date in file:
                df = file_to_df(file)
                if df is not None:
                    pipe_hits = preprocessor_hits.fit_transform(df)

                    # Удаление строк, у которых session_id отсутствует в таблице db_sessions
                    pipe_hits = pipe_hits[pipe_hits.session_id.isin(columns)]

                    insert_into_table(pipe_hits, 'db_hits', file, cur, conn)


if __name__ == "__main__":
    pipeline()
