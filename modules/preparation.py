import pandas as pd
import logging

from datetime import datetime
from modules.DDL import parse_ini


logging.basicConfig(level=logging.INFO)
path_info, _ = parse_ini() # Считываем путь к проекту из конфигурационного файла


def missing_values(
        df: pd.DataFrame
) -> pd.Series:
    """
    Функция поиска пропущенных значений в процентах для каждого признака в датафрейме
    """

    miss_values = None
    if df is not None:
        miss_values = (df.isna().sum() / len(df) * 100).sort_values(ascending=False)
    return miss_values


def save_to_csv(
        df: pd.DataFrame,
        file_name: str
) -> None:
    """
    Функция сохранения датафрейма в файл csv
    """

    try:
        df.to_csv(f'{path_info}/data/prep_data/{file_name}.csv', index=False)
        logging.info(f" * SUCCESS *: Save data \'{file_name}.csv\' to \'{path_info}/data/prep_data\' complete.")
    except Exception as e:
        logging.error(f"{type(e).__name__}: '{e}' occurred.")


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


def prep_sessions(
        df_sessions: pd.DataFrame,
        path: str
) -> pd.DataFrame:
    """
    Функция обработки датасета sessions
    """

    if df_sessions is not None:
        try:
            # Удаление колонок с процентом пропущенных значений более 20
            df_sessions = filter_data_sessions(df_sessions)

            # Заполнение пропусков категориальных признаков
            df_sessions = fill_cat_col_sessions(df_sessions)

            # Приведение типов
            df_sessions = corr_types_sessions(df_sessions)

            # Удаление дубикатов
            df_sessions = df_sessions.drop_duplicates()

            logging.info(f" * SUCCESS *: Preparation data \'{path.split('/')[-1].split('.')[0]}\' complete.")
        except Exception as e:
            logging.error(f"{type(e).__name__}: '{e}' occurred.")
    else:
        logging.info(f" Dataframe \'{path.split('/')[-1].split('.')[0]}\' is empty.")
    return df_sessions


def prep_hits(
        df_hits: pd.DataFrame,
        path: str
) -> pd.DataFrame:
    """
    Функция обработки датасета hits
    """

    if df_hits is not None:
        try:
            # Удаление колонок с процентом пропущенных значений более 20 и неинформативной колонки 'hit_type'
            # с единственным значением
            df_hits = filter_data_hits(df_hits)

            # Заполнение пропусков категориальных признаков
            df_hits = fill_cat_col_hits(df_hits)

            # Приведение типов
            df_hits = corr_types_hits(df_hits)

            # Удаление дубикатов
            df_hits = df_hits.drop_duplicates()

            logging.info(f" * SUCCESS *: Preparation data \'{path.split('/')[-1].split('.')[0]}\' complete.")
        except Exception as e:
            logging.error(f"{type(e).__name__}: '{e}' occurred.")
    else:
        logging.info(f" Dataframe \'{path.split('/')[-1].split('.')[0]}\' is empty.")
    return df_hits

def data_prep() -> None:
    """
    Главная функция
    """

    logging.info('\n-------------------Data preparation-------------------\n')

    path_sessions = f'{path_info}/data/main_data/ga_sessions.csv'
    path_hits = f'{path_info}/data/main_data/ga_hits.csv'

    # Обработка sessions
    df_sessions = pd.read_csv(path_sessions)
    df_sessions = prep_sessions(df_sessions, path_sessions)
    save_to_csv(df_sessions, 'ga_sessions_prep')

    # Обработка hits
    df_hits = pd.read_csv(path_hits)
    df_hits = prep_hits(df_hits, path_hits)
    save_to_csv(df_hits, 'ga_hits_prep')


if __name__ == "__main__":
    data_prep()
